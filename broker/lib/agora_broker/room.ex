defmodule AgoraBroker.Room do
  use GenServer, restart: :temporary
  alias AgoraBroker.Policy
  @max_queue 128
  @max_bytes 16 * 1024 * 1024
  @retryable_save_errors ~w(BROKER_BUSY BROKER_BACKEND_UNAVAILABLE BROKER_QUEUE_FULL RATE_LIMITED QUERY_OUTCOME_UNKNOWN QUERY_UNAVAILABLE STORAGE_UNAVAILABLE CANVAS_WRITE_CONFLICT)
  def start_link(key),
    do:
      GenServer.start_link(__MODULE__, key,
        name: {:via, Registry, {AgoraBroker.RoomRegistry, key}}
      )

  def ensure(key) do
    case Registry.lookup(AgoraBroker.RoomRegistry, key) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(AgoraBroker.RoomSupervisor, {__MODULE__, key}) do
          {:error, {:already_started, pid}} -> {:ok, pid}
          result -> result
        end
    end
  end

  def join(room, pid, state), do: GenServer.call(room, {:join, pid, state})
  def leave(room, pid), do: GenServer.cast(room, {:leave, pid})

  def submit(room, pid, event) do
    GenServer.call(room, {:event, pid, event}, 10_000)
  catch
    :exit, {:timeout, _} -> {:error, "QUERY_OUTCOME_UNKNOWN"}
    :exit, {:noproc, _} -> {:error, "BROKER_BACKEND_UNAVAILABLE"}
    :exit, _ -> {:error, "QUERY_OUTCOME_UNKNOWN"}
  end

  def control(id, command) do
    for {{_port, canvas}, pid, _} <-
          Registry.select(AgoraBroker.RoomRegistry, [
            {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
          ]),
        is_nil(id) or canvas == id,
        do: GenServer.cast(pid, {:control, command})

    :ok
  end

  def init(key) do
    Process.send_after(self(), :watermark, 1000)
    Process.send_after(self(), :lease, 30_000)

    {:ok,
     %{
       key: key,
       sessions: %{},
       refs: %{},
       peers: %{},
       revision: 0,
       queue: :queue.new(),
       queued_bytes: 0,
       active: nil
     }}
  end

  def handle_call({:join, pid, client}, from, state) do
    {port, canvas} = state.key

    if client.ws_port == port and client.route["canvas_id"] == canvas and
         client.identity["canvas_id"] == canvas do
      join_client(pid, client, from, state)
    else
      {:reply, {:error, "CANVAS_ROUTE_MISMATCH"}, state}
    end
  end

  def handle_call({:event, pid, event}, _, state) do
    case state.sessions[pid] do
      nil -> {:reply, {:error, "SESSION_NOT_FOUND"}, state}
      client -> handle_event(client, event, state)
    end
  end

  defp join_client(pid, client, _, state) do
    ref = Process.monitor(pid)
    hash = Policy.id()
    client = Map.merge(client, %{pid: pid, proof: hash, peer_id: nil, bound: nil})

    state = %{
      state
      | sessions: Map.put(state.sessions, pid, client),
        refs: Map.put(state.refs, ref, pid)
    }

    response =
      if client.rtc,
        do: %{"type" => "rtc_ready"},
        else:
          client.response
          |> Map.put("rtc_canvas_connection_id", client.route["connection_id"])
          |> Map.put("rtc_canvas_connection_hash", hash)

    if not client.rtc and Map.has_key?(client, :sync_client_id) do
      # Register first, then serialize the snapshot with mutations. No join gap.
      event = %{
        "type" => "canvas_sync",
        "client_id" => client.sync_client_id,
        "request_id" => Policy.id()
      }

      if :queue.len(state.queue) >= @max_queue do
        send(pid, {:close, :slow_consumer})
        {:reply, :ok, state}
      else
        entry = %{client: client, event: event, bytes: 0, initial: response}
        {:reply, :ok, dispatch(%{state | queue: :queue.in(entry, state.queue)})}
      end
    else
      deliver(pid, response)
      {:reply, :ok, state}
    end
  end

  defp handle_event(client, %{"type" => "ping"}, state) do
    deliver(client.pid, %{"type" => "pong"})
    {:reply, :ok, state}
  end

  defp handle_event(_client, %{"type" => "pong"}, state), do: {:reply, :ok, state}

  defp handle_event(%{rtc: true} = client, %{"type" => "rtc_join"} = event, state) do
    bound =
      Enum.find_value(state.sessions, fn {pid, candidate} ->
        if not candidate.rtc and candidate.route["connection_id"] == event["canvas_connection_id"] and
             candidate.proof == event["canvas_connection_hash"] and
             candidate.identity["user_id"] == client.identity["user_id"],
           do: pid
      end)

    cond do
      is_nil(bound) or event["canvas_protocol"] != 2 ->
        {:reply, {:error, "RTC_CANVAS_CONNECTION_INVALID"}, state}

      event["canvas_transport"] != "broker-v1" ->
        {:reply, {:error, "RTC_CANVAS_TRANSPORT_REQUIRED"}, state}

      client.peer_id != nil ->
        {:reply, {:error, "RTC_ALREADY_JOINED"}, state}

      true ->
        id = Policy.id()
        client = %{client | peer_id: id, bound: bound}

        state =
          %{
            state
            | sessions: Map.put(state.sessions, client.pid, client),
              peers: Map.put(state.peers, id, client.pid)
          }

        list_peers(client, state)
        public = Policy.public(client.identity, id)

        for {_id, pid} <- state.peers,
            pid != client.pid,
            do: deliver(pid, %{"type" => "rtc_peer_joined", "peer" => public})

        {:reply, :ok, state}
    end
  end

  defp handle_event(%{rtc: true, peer_id: id} = client, %{"type" => "rtc_list"}, state)
       when is_binary(id) do
    list_peers(client, state)
    {:reply, :ok, state}
  end

  defp handle_event(%{rtc: true} = client, %{"type" => "rtc_disconnect"}, state) do
    state = detach(client.pid, state)
    deliver(client.pid, %{"type" => "rtc_disconnected"})
    {:reply, :ok, state}
  end

  defp handle_event(%{rtc: true, peer_id: id} = client, %{"type" => "rtc_signal"} = event, state)
       when is_binary(id) do
    target = state.peers[event["peer_id"]]
    action = event["action"]
    field = if action == "candidate", do: "candidate", else: "description"

    if target && target != client.pid && action in ["offer", "answer", "candidate"] &&
         is_map(event[field]) && byte_size(Jason.encode!(event)) <= 262_144 do
      public = Policy.public(client.identity, id)

      deliver(target, %{
        "type" => "rtc_signal",
        "action" => action,
        "from_peer_id" => id,
        "from_nickname" => public["nickname"],
        "from_tag_number" => public["tag_number"],
        "from_groups" => public["groups"],
        "from_is_admin" => public["is_admin"],
        field => event[field]
      })

      {:reply, :ok, state}
    else
      {:reply, {:error, "RTC_SIGNAL_INVALID"}, state}
    end
  end

  # The bound collaboration socket relays canvas data; rtc_signal is reserved
  # for call negotiation. Sender identity and room membership come from auth.
  defp handle_event(
         %{rtc: true, peer_id: id} = client,
         %{"type" => "canvas_relay", "payload" => payload} = event,
         state
       )
       when is_binary(id) and is_map(payload) do
    targets = Map.get(event, "peer_ids", Map.keys(state.peers) -- [id])
    ephemeral = payload["type"] in ["cursor", "laser", "editor_presence"]

    cond do
      not Map.has_key?(state.sessions, client.bound) or state.peers[id] != client.pid ->
        {:reply, {:error, "CANVAS_RELAY_UNBOUND"}, state}

      not is_list(targets) or length(targets) > 512 or
          not Enum.all?(targets, &(is_binary(&1) and &1 != id)) ->
        {:reply, {:error, "CANVAS_RELAY_TARGET_INVALID"}, state}

      not ephemeral ->
        {:reply, {:error, "CANVAS_RELAY_TYPE_INVALID"}, state}

      not Enum.all?(targets, &Map.has_key?(state.peers, &1)) ->
        {:reply, {:error, "CANVAS_RELAY_TARGET_INVALID"}, state}

      ephemeral and byte_size(Jason.encode!(payload)) > 65_536 ->
        {:reply, {:error, "CANVAS_RELAY_TOO_LARGE"}, state}

      not relay_records_visible?(payload, targets, state) ->
        {:reply, {:error, "ITEM_ACCESS_DENIED"}, state}

      true ->
        frame = %{
          "type" => "canvas_relay",
          "peer" => Policy.public(client.identity, id),
          "payload" => payload
        }

        for target <- Enum.uniq(targets), do: deliver(state.peers[target], frame)
        {:reply, :ok, state}
    end
  end

  defp handle_event(%{rtc: true}, _, state), do: {:reply, {:error, "RTC_SIGNAL_INVALID"}, state}

  defp handle_event(client, event, state) do
    type = event["type"]

    cond do
      type not in [
        "canvas_ops",
        "canvas_sync",
        "chat",
        "chat_history",
        "mentions_get",
        "mention_read",
        "canvas_settings_get",
        "canvas_settings_update"
      ] ->
        {:reply, {:error, "QUERY_TYPE_INVALID"}, state}

      true ->
        event =
          Map.drop(event, [
            "user_id",
            "userId",
            "sender_id",
            "senderId",
            "sender_user_id",
            "canvas_id",
            "capability",
            "identity",
            "principal",
            "token",
            "route"
          ])

        event = Map.put(event, "request_id", valid_id(event["request_id"]))
        size = byte_size(Jason.encode!(event))

        pending = pending_save(state, client, event)

        cond do
          pending == :same ->
            {:reply, :ok, state}

          pending == :mismatch ->
            {:reply, {:error, "CLIENT_REQUEST_MISMATCH"}, state}

          :queue.len(state.queue) >= @max_queue or state.queued_bytes + size > @max_bytes ->
            {:reply, {:error, "BROKER_BUSY"}, state}

          true ->
            entry = %{client: client, event: event, bytes: size}

            state =
              %{
                state
                | queue: :queue.in(entry, state.queue),
                  queued_bytes: state.queued_bytes + size
              }
              |> dispatch()

            {:reply, :ok, state}
        end
    end
  end

  def handle_cast({:leave, pid}, state), do: {:noreply, remove(pid, state)}

  def handle_cast({:control, command}, state) do
    for {pid, client} <- state.sessions,
        is_nil(command["user_id"]) or command["user_id"] == client.identity["user_id"] do
      deliver(pid, %{
        "type" => "server_reconnect",
        "reason" => "access_revoked",
        "retry_after_ms" => 100
      })

      send(pid, {:close, :revoked})
    end

    {:noreply, state}
  end

  def handle_info({ref, result}, %{active: %{ref: ref, entry: entry}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | active: nil}
    route = route(entry.client, entry.event)

    state =
      case result do
        {:ok, %{"route" => ^route, "response" => response} = result} ->
          if retry_save?(entry, response["code"]) and response["ok"] != true do
            defer_save(entry, response["code"], state)
          else
            response =
              if Map.has_key?(entry, :initial) and response["type"] == "canvas_snapshot",
                do: Map.merge(entry.initial, Map.put(response, "type", "init_items")),
                else: response

            if Map.has_key?(entry, :initial) and response["type"] != "init_items",
              do: send(entry.client.pid, {:close, :snapshot_failed})

            state_revision =
              max(state.revision, response["server_revision"] || response["revision"] || 0)

            if Map.has_key?(state.sessions, entry.client.pid),
              do: deliver(entry.client.pid, response)

            for broadcast <- result["broadcasts"] || [],
                {pid, client} <- state.sessions,
                not client.rtc and
                  Policy.receives?(client.identity, broadcast) do
              deliver(pid, filtered_event(broadcast, client.identity))
              if broadcast["close"], do: send(pid, {:close, :settings_changed})
            end

            if entry.event["type"] == "canvas_ops" and Map.get(entry, :attempts, 0) > 0 and
                 response["ok"] != true,
               do: force_sync(state)

            %{state | revision: state_revision}
          end

        {:error, code} when is_binary(code) ->
          failed_query(entry, code, state)

        _ ->
          failed_query(entry, "QUERY_OUTCOME_UNKNOWN", state)
      end

    {:noreply, dispatch(state)}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{active: %{ref: ref, entry: entry}} = state
      ) do
    {:noreply, dispatch(failed_query(entry, "QUERY_OUTCOME_UNKNOWN", %{state | active: nil}))}
  end

  def handle_info({:retry_save, token}, %{active: %{retry: token, entry: entry}} = state) do
    state = %{state | active: nil}

    if save_identity_valid?(entry) do
      {:noreply, start_entry(entry, state)}
    else
      force_sync(state)
      deliver(entry.client.pid, request_error(entry.event, "QUERY_UNAUTHORIZED"))
      {:noreply, dispatch(state)}
    end
  end

  def handle_info({:retry_save, _}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _} -> {:noreply, state}
      {pid, refs} -> {:noreply, remove(pid, %{state | refs: refs})}
    end
  end

  def handle_info(:watermark, state) do
    Process.send_after(self(), :watermark, 1000)

    for {pid, client} <- state.sessions,
        not client.rtc,
        do: deliver(pid, %{"type" => "canvas_watermark", "revision" => state.revision})

    {:noreply, state}
  end

  def handle_info(:lease, state) do
    Process.send_after(self(), :lease, 30_000)

    case Enum.find(state.sessions, fn {_pid, client} -> not client.rtc end) do
      nil ->
        if map_size(state.sessions) == 0 and is_nil(state.active),
          do: {:stop, :normal, state},
          else: {:noreply, state}

      {_pid, client} ->
        entry = %{
          client: client,
          event: %{"type" => "broker_lease", "request_id" => Policy.id()},
          bytes: 0
        }

        if :queue.len(state.queue) < @max_queue,
          do: {:noreply, dispatch(%{state | queue: :queue.in(entry, state.queue)})},
          else: {:noreply, state}
    end
  end

  defp relay_records_visible?(%{"records" => records}, targets, state) when is_map(records) do
    Enum.all?(targets, fn target ->
      identity = state.sessions[state.peers[target]].identity

      Enum.all?(records, fn {_id, record} ->
        is_map(record) and
          Policy.visible?(
            identity,
            get_in(record, ["item", "permission"]) || record["permission"]
          )
      end)
    end)
  end

  defp relay_records_visible?(%{"records" => _}, _, _), do: false
  defp relay_records_visible?(_, _, _), do: true

  defp dispatch(%{active: active} = state) when not is_nil(active), do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        state

      {{:value, entry}, rest} ->
        state = %{state | queue: rest, queued_bytes: state.queued_bytes - entry.bytes}

        cond do
          not Map.has_key?(state.sessions, entry.client.pid) and
              entry.event["type"] != "canvas_ops" ->
            dispatch(state)

          true ->
            start_entry(entry, state)
        end
    end
  end

  defp start_entry(entry, state) do
    backend = Application.get_env(:agora_broker, :backend, AgoraBroker.Backend)

    task =
      try do
        {:ok,
         Task.Supervisor.async_nolink(AgoraBroker.Tasks, fn ->
           backend.query(%{
             "route" => route(entry.client, entry.event),
             "identity" => entry.client.identity,
             "ws_port" => entry.client.ws_port,
             "event" => entry.event
           })
         end)}
      rescue
        RuntimeError -> {:error, "BROKER_BUSY"}
      end

    case task do
      {:ok, task} -> %{state | active: %{ref: task.ref, entry: entry}}
      {:error, code} -> dispatch(failed_query(entry, code, state))
    end
  end

  defp save_identity_valid?(entry),
    do:
      Map.get(entry.client.identity, "expires_at", System.system_time(:second) + 1) >
        System.system_time(:second)

  defp retry_save?(entry, code),
    do:
      entry.event["type"] == "canvas_ops" and code in @retryable_save_errors and
        save_identity_valid?(entry)

  defp defer_save(entry, code, state) do
    deliver(entry.client.pid, request_error(entry.event, code))
    token = make_ref()
    Process.send_after(self(), {:retry_save, token}, 1000)

    %{
      state
      | active: %{ref: nil, entry: Map.update(entry, :attempts, 1, &(&1 + 1)), retry: token}
    }
  end

  defp failed_query(entry, code, state) do
    if retry_save?(entry, code) do
      defer_save(entry, code, state)
    else
      deliver(entry.client.pid, request_error(entry.event, code))

      if entry.event["type"] == "canvas_ops" and Map.get(entry, :attempts, 0) > 0,
        do: force_sync(state)

      state
    end
  end

  defp force_sync(state) do
    for {pid, client} <- state.sessions,
        not client.rtc,
        do:
          deliver(pid, %{
            "type" => "canvas_watermark",
            "revision" => state.revision,
            "force_sync" => true
          })
  end

  defp pending_save(
         state,
         client,
         %{"type" => "canvas_ops", "client_id" => id, "request_count" => count} = event
       )
       when is_binary(id) and is_integer(count) do
    entries = if(state.active, do: [state.active.entry], else: []) ++ :queue.to_list(state.queue)

    match =
      Enum.find(entries, fn entry ->
        entry.event["type"] == "canvas_ops" and
          entry.client.identity["user_id"] == client.identity["user_id"] and
          entry.event["client_id"] == id and entry.event["request_count"] == count
      end)

    cond do
      is_nil(match) -> nil
      match.event == event -> :same
      true -> :mismatch
    end
  end

  defp pending_save(_, _, _), do: nil

  def format_status(status),
    do: Map.update(status, :state, %{}, &Map.drop(&1, [:sessions, :queue, :active]))

  defp request_error(event, code),
    do: %{
      "type" => "error",
      "ok" => false,
      "code" => code,
      "request_id" => event["request_id"]
    }

  defp remove(pid, state) do
    state = detach(pid, state)

    for {other, client} <- state.sessions,
        client.bound == pid,
        do: send(other, {:close, :canvas_socket_closed})

    %{state | sessions: Map.delete(state.sessions, pid)}
  end

  defp detach(pid, state) do
    case state.sessions[pid] do
      %{peer_id: id} = client when is_binary(id) ->
        state = %{
          state
          | peers: Map.delete(state.peers, id),
            sessions: Map.put(state.sessions, pid, %{client | peer_id: nil, bound: nil})
        }

        for {_id, other} <- state.peers,
            do: deliver(other, %{"type" => "rtc_peer_left", "peer_id" => id})

        state

      _ ->
        state
    end
  end

  defp list_peers(client, state) do
    peers =
      for {id, pid} <- state.peers,
          pid != client.pid,
          do: Policy.public(state.sessions[pid].identity, id)

    deliver(client.pid, %{
      "type" => "rtc_peers",
      "self_peer_id" => client.peer_id,
      "peers" => peers,
      "canvas_protocol" => 2
    })
  end

  defp filtered_event(%{"event" => %{"type" => "canvas_commit"} = event} = broadcast, identity) do
    permissions = broadcast["permissions"] || %{}
    previous = broadcast["previous_permissions"] || %{}

    if Map.has_key?(event, "translations") do
      translations =
        for move <- event["translations"],
            ids = Enum.filter(move["ids"], &Policy.visible?(identity, permissions[&1])),
            ids != [],
            do: Map.put(move, "ids", ids)

      Map.put(event, "translations", translations)
    else
      items =
        Enum.reduce(event["items"] || %{}, %{}, fn {id, item}, result ->
          cond do
            not is_nil(item) and Policy.visible?(identity, permissions[id]) ->
              Map.put(result, id, item)

            Policy.visible?(identity, previous[id]) ->
              Map.put(result, id, nil)

            true ->
              result
          end
        end)

      Map.put(event, "items", items)
    end
  end

  defp filtered_event(broadcast, _identity), do: broadcast["event"]

  defp route(client, event), do: Map.put(client.route, "request_id", event["request_id"])
  defp valid_id(id) when is_binary(id) and byte_size(id) in 1..128, do: id
  defp valid_id(_), do: Policy.id()

  defp deliver(pid, payload) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, count} when count < 64 -> send(pid, {:deliver, payload})
      {:message_queue_len, _} -> send(pid, {:close, :slow_consumer})
      _ -> :ok
    end
  end
end
