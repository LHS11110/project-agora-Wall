defmodule AgoraBroker.Room do
  use GenServer, restart: :temporary
  alias AgoraBroker.Policy
  @max_queue 128
  @max_bytes 16 * 1024 * 1024
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
  def submit(room, pid, event), do: GenServer.call(room, {:event, pid, event}, 2_000)

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
    Process.send_after(self(), :lease, 30_000)

    {:ok,
     %{
       key: key,
       sessions: %{},
       refs: %{},
       peers: %{},
       term: Policy.id(),
       host: "",
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

    deliver(pid, response)
    {:reply, :ok, state}
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
      is_nil(bound) or event["host_protocol"] != 1 ->
        {:reply, {:error, "RTC_CANVAS_CONNECTION_INVALID"}, state}

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
          |> elect()

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

  defp handle_event(%{rtc: true}, _, state), do: {:reply, {:error, "RTC_SIGNAL_INVALID"}, state}

  defp handle_event(client, event, state) do
    type = event["type"]

    authorized_host =
      Enum.any?(state.peers, fn {peer, pid} ->
        peer == state.host && state.sessions[pid].bound == client.pid
      end)

    cond do
      type == "host_item_batch" && (not authorized_host or event["host_term"] != state.term) ->
        {:reply, {:error, "HOST_FENCED"}, state}

      type not in [
        "host_item_batch",
        "crud",
        "crud_batch",
        "chat",
        "chat_history",
        "canvas_settings_get",
        "canvas_settings_update"
      ] ->
        {:reply, {:error, "QUERY_TYPE_INVALID"}, state}

      type in ["crud", "crud_batch"] && event["action"] != "read" && not authorized_host ->
        {:reply, {:error, "HOST_FENCED"}, state}

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

        if :queue.len(state.queue) >= @max_queue or state.queued_bytes + size > @max_bytes do
          {:reply, {:error, "BROKER_BUSY"}, state}
        else
          entry = %{client: client, event: event, bytes: size, term: state.term}

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

    case result do
      {:ok, %{"route" => ^route, "response" => response} = result} ->
        response =
          if entry.event["type"] == "host_item_batch" and response["type"] == "error",
            do: Map.put(response, "type", "host_batch_result"),
            else: response

        if Map.has_key?(state.sessions, entry.client.pid), do: deliver(entry.client.pid, response)

        for broadcast <- result["broadcasts"] || [],
            {pid, client} <- state.sessions,
            not client.rtc and
              (broadcast["all"] == true or
                 Policy.visible?(client.identity, broadcast["permission"])) do
          deliver(pid, broadcast["event"])
          if broadcast["close"], do: send(pid, {:close, :settings_changed})
        end

      {:error, code} when is_binary(code) ->
        deliver(entry.client.pid, request_error(entry.event, code))

      _ ->
        deliver(entry.client.pid, %{
          "type" =>
            if(entry.event["type"] == "host_item_batch", do: "host_batch_result", else: "error"),
          "ok" => false,
          "code" => "QUERY_OUTCOME_UNKNOWN",
          "request_id" => route["request_id"]
        })
    end

    {:noreply, dispatch(state)}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{active: %{ref: ref, entry: entry}} = state
      ) do
    deliver(entry.client.pid, %{
      "type" =>
        if(entry.event["type"] == "host_item_batch", do: "host_batch_result", else: "error"),
      "ok" => false,
      "code" => "QUERY_OUTCOME_UNKNOWN",
      "request_id" => entry.event["request_id"]
    })

    {:noreply, dispatch(%{state | active: nil})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _} -> {:noreply, state}
      {pid, refs} -> {:noreply, remove(pid, %{state | refs: refs})}
    end
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
          bytes: 0,
          term: state.term
        }

        if :queue.len(state.queue) < @max_queue,
          do: {:noreply, dispatch(%{state | queue: :queue.in(entry, state.queue)})},
          else: {:noreply, state}
    end
  end

  defp dispatch(%{active: active} = state) when not is_nil(active), do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        state

      {{:value, entry}, rest} ->
        state = %{state | queue: rest, queued_bytes: state.queued_bytes - entry.bytes}

        cond do
          not Map.has_key?(state.sessions, entry.client.pid) ->
            dispatch(state)

          entry.event["type"] == "host_item_batch" && entry.term != state.term ->
            deliver(entry.client.pid, %{
              "type" => "host_batch_result",
              "ok" => false,
              "code" => "HOST_FENCED",
              "request_id" => entry.event["request_id"]
            })

            dispatch(state)

          true ->
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
              {:ok, task} ->
                %{state | active: %{ref: task.ref, entry: entry}}

              {:error, code} ->
                deliver(entry.client.pid, request_error(entry.event, code))
                dispatch(state)
            end
        end
    end
  end

  def format_status(status),
    do: Map.update(status, :state, %{}, &Map.drop(&1, [:sessions, :queue, :active]))

  defp request_error(event, code),
    do: %{
      "type" => if(event["type"] == "host_item_batch", do: "host_batch_result", else: "error"),
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

        elect(state)

      _ ->
        state
    end
  end

  defp elect(state) do
    peers =
      Enum.map(state.peers, fn {id, pid} -> Policy.public(state.sessions[pid].identity, id) end)

    state = %{state | term: Policy.id(), host: Policy.elect(peers)}

    for {_id, pid} <- state.peers,
        do: deliver(pid, %{"type" => "rtc_host_changed", "host" => host(state)})

    state
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
      "host" => host(state)
    })
  end

  defp host(state),
    do: %{
      "protocol" => 1,
      "term" => state.term,
      "peer_id" => state.host,
      "members" =>
        Enum.map(state.peers, fn {id, pid} -> Policy.public(state.sessions[pid].identity, id) end)
    }

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
