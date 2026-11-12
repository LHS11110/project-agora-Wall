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

  def broadcast_workspace(id, payload) when is_map(payload) do
    for {{_port, canvas}, pid, _} <-
          Registry.select(AgoraBroker.RoomRegistry, [
            {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
          ]),
        is_nil(id) or canvas == id,
        do: GenServer.cast(pid, {:workspace_broadcast, payload})

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
       settings_revision: nil,
       refresh_pending: 0,
       queue: :queue.new(),
       queued_bytes: 0,
       active: nil,
       runtime_tasks: %{}
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

    client =
      Map.merge(client, %{
        pid: pid,
        proof: hash,
        peer_id: nil,
        bound: nil,
        voice_active: false,
        voice_muted: false,
        screen_sharing: false
      })

    state = %{
      state
      | sessions: Map.put(state.sessions, pid, client),
        refs: Map.put(state.refs, ref, pid)
    }

    if client.response["participant_joined"] == true do
      for {other_pid, other} <- state.sessions, other_pid != pid and not other.rtc,
        do: deliver(other_pid, %{"type" => "canvas_participants_changed"})
    end

    response =
      if client.rtc,
        do: %{"type" => "rtc_ready"},
        else:
          client.response
          |> Map.put("rtc_canvas_connection_id", client.route["connection_id"])
          |> Map.put("rtc_canvas_connection_hash", hash)

    state =
      if not client.rtc and not is_nil(state.settings_revision) and
           client.identity["settings_revision"] != state.settings_revision,
         do: enqueue_access_refresh(state, [client], true),
         else: state

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

        acl =
          Map.take(state.sessions[bound].identity, [
            "groups",
            "member_groups",
            "write_groups",
            "execute_groups",
            "is_admin",
            "group_bits",
            "group_masks",
            "settings_revision"
          ])

        identity =
          client.identity
          |> Map.drop(["_group_index", "_read_mask"])
          |> Map.merge(acl)
          |> Policy.compile()

        client = %{client | peer_id: id, bound: bound, identity: identity}

        state =
          %{
            state
            | sessions: Map.put(state.sessions, client.pid, client),
              peers: Map.put(state.peers, id, client.pid)
          }

        list_peers(client, state)
        public = voice_public(client, id)

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

  defp handle_event(
         %{rtc: true, peer_id: id} = client,
         %{"type" => "voice_state", "active" => active, "muted" => muted} = event,
         state
       )
       when is_binary(id) and is_boolean(active) and is_boolean(muted) do
    sharing = Map.get(event, "screen_sharing", false)
    client = %{client | voice_active: active, voice_muted: active and muted, screen_sharing: active and sharing == true}
    state = %{state | sessions: Map.put(state.sessions, client.pid, client)}

    for {_id, pid} <- state.peers,
        do:
          deliver(pid, %{
            "type" => "voice_state",
            "peer_id" => id,
            "active" => active,
            "muted" => client.voice_muted,
            "screen_sharing" => client.screen_sharing
          })

    {:reply, :ok, state}
  end

  defp handle_event(%{rtc: true, peer_id: id} = client, %{"type" => "rtc_signal"} = event, state)
       when is_binary(id) do
    target = state.peers[event["peer_id"]]
    action = event["action"]
    field = if action == "candidate", do: "candidate", else: "description"

    if target && target != client.pid && action in ["offer", "answer", "candidate"] &&
         is_map(event[field]) && byte_size(Jason.encode!(event)) <= 262_144 do
      public = voice_public(client, id)

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
      state.refresh_pending > 0 ->
        {:reply, :ok, state}

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
    client = if type in ["canvas_sync", "canvas_ops"] and event["delta_commits"] == true,
      do: %{client | identity: Map.put(client.identity, "delta_commits", true)}, else: client
    state = if Map.has_key?(state.sessions, client.pid), do: put_in(state.sessions[client.pid], client), else: state

    cond do
      type not in [
        "canvas_ops",
        "canvas_sync",
        "workspace_ops",
        "workspace_sync",
        "workspace_files",
        "runtime_execute",
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

        cond do
          type == "workspace_files" and not workspace_files_allowed?(client.identity, event, size) ->
            {:reply, {:error, "WORKSPACE_ACCESS_DENIED"}, state}

          type == "runtime_execute" and not runtime_request_valid?(event) ->
            {:reply, {:error, "RUNTIME_REQUEST_INVALID"}, state}

          type == "runtime_execute" and not runtime_allowed?(client.identity, event) ->
            {:reply, {:error, "RUNTIME_EXECUTION_DENIED"}, state}

          type == "runtime_execute" and size > 800_000 ->
            {:reply, {:error, "RUNTIME_REQUEST_TOO_LARGE"}, state}

          type == "runtime_execute" and runtime_busy?(event, state) ->
            {:reply, {:error, "RUNTIME_BUSY"}, state}

          type == "runtime_execute" ->
            {:reply, :ok, start_runtime_request(client, event, state)}

          true ->
            queue_event(client, event, size, state)
        end
    end
  end

  defp queue_event(client, event, size, state) do
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

  defp runtime_busy?(event,state) do
    service=event["action"]=="service"
    Enum.count(state.runtime_tasks,fn {_,entry}->(entry.event["action"]=="service")==service end)>=(if service,do: 4,else: 1)
  end

  defp workspace_files_allowed?(identity, event, size) do
    read = event["operation"] in ["stat", "list", "read"]
    allowed = event["operation"] in ["stat", "list", "read", "write", "mkdir", "rename", "delete"]
    allowed and size <= 700_000 and
      (identity["is_admin"] == true or
        ((identity["groups"] || []) != [] and (read or (identity["write_groups"] || []) != [])))
  end

  defp runtime_allowed?(identity, event) do
    if event["action"]=="service" do
      request=event["service"]||%{}
      read=request["operation"]=="resource" or (request["operation"] in [nil,"api"] and (request["method"] in [nil,"GET"] or String.starts_with?(request["path"]||"", "/github") or request["path"] in ["/access","/link-preview"]))
      manager=String.starts_with?(request["path"]||"", "/bots") or request["path"] in ["/github/bind","/github/permissions","/people"] or (request["path"]=="" and request["method"]=="DELETE") or (request["path"]=="/image" and not read)
      identity["is_admin"]==true or (not manager and (if request["path"]=="/runtime/terminal/ticket",do: Policy.can_execute?(identity),else: (if read,do: (identity["groups"]||[])!=[],else: (identity["write_groups"]||[])!=[])))
    else
    if event["action"] == "manage" and get_in(event, ["management", "action"]) == "settings_get" do
      identity["is_admin"] == true or (identity["groups"] || []) != []
    else
      manage = event["action"] == "manage" or (event["action"] == "notebook" and event["operation"] == "prepare")
      Policy.can_execute?(identity) and (not manage or identity["is_admin"] == true or (identity["write_groups"] || []) != [])
    end
  end
  end

  defp runtime_request_valid?(event) do
    action = event["action"]
    code = event["code"]
    object_id = event["object_id"]
    if action=="service" do
      is_map(event["service"]) and is_binary(event["service"]["path"]) and byte_size(event["service"]["path"])<=2048 and event["service"]["operation"] in [nil,"api","resource","upload_begin","upload_chunk","upload_finish","upload_cancel"]
    else
    if action == "manage" do
      is_map(event["management"]) and event["management"]["action"] in ["settings_get", "settings_set", "resources_set", "disk_set", "java_dependencies", "reset"]
    else
      is_binary(action) and action in ["execute", "build", "notebook"] and
        is_binary(code) and byte_size(code) <= 65_536 and
        is_binary(object_id) and byte_size(object_id) in 1..128 and
        (action == "notebook" or event["language"] in ["python", "javascript", "c", "cpp", "bash", "java"]) and
        (action != "notebook" or
          (event["operation"] in [nil, "execute", "prepare", "reset"] and
           is_binary(event["notebook_id"]) and byte_size(event["notebook_id"]) in 1..128 and
           (event["operation"] in ["prepare", "reset"] or (is_binary(event["cell_id"]) and byte_size(event["cell_id"]) in 1..128)))) and
        (is_nil(event["stdin"]) or (is_binary(event["stdin"]) and byte_size(event["stdin"]) <= 65_536))
    end
  end
  end

  defp start_runtime_request(client, event, state) do
    backend = Application.get_env(:agora_broker, :backend, AgoraBroker.Backend)
    route = route(client, event)
    input = %{
      "route" => route,
      "identity" => Map.drop(client.identity, ["_group_index", "_read_mask"]),
      "ws_port" => client.ws_port,
      "runtime_sink" => self(),
      "event" => event
    }

    task =
      try do
        {:ok, Task.Supervisor.async_nolink(AgoraBroker.Tasks, fn -> backend.query(input) end)}
      rescue
        RuntimeError -> {:error, "BROKER_BUSY"}
      end

    case task do
      {:ok, task} ->
        task_info = %{client: client, event: event, route: route, pid: task.pid}
        put_in(state.runtime_tasks[task.ref], task_info)

      {:error, code} ->
        deliver(client.pid, request_error(event, code))
        state
    end
  end

  defp runtime_response(entry, {:ok, %{"route" => route, "response" => response}}) when is_map(response) do
    cond do
      route != entry.route -> runtime_error(entry.event, "QUERY_ROUTE_INVALID")
      response["type"] == "runtime_result" -> Map.put_new(response, "request_id", entry.event["request_id"])
      true -> runtime_error(entry.event, response["code"] || "QUERY_UNAVAILABLE")
    end
  end

  defp runtime_response(entry, {:error, code}) when is_binary(code),
    do: runtime_error(entry.event, code)

  defp runtime_response(entry, _), do: runtime_error(entry.event, "QUERY_OUTCOME_UNKNOWN")

  defp runtime_error(event, code),
    do: %{
      "type" => "runtime_result",
      "ok" => false,
      "code" => code,
      "request_id" => event["request_id"]
    }

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

  def handle_cast({:workspace_broadcast, payload}, state) when is_map(payload) do
    frame = Map.put_new(payload, "type", "workspace_commit")

    for {pid, client} <- state.sessions,
        not client.rtc do
      deliver(pid, frame)
    end

    {:noreply, state}
  end

  def handle_info(
        {ref, result},
        %{active: %{ref: ref, entry: %{access_refresh: true} = entry}} = state
      ) do
    Process.demonitor(ref, [:flush])
    route = route(entry.client, entry.event)
    state = %{state | active: nil}

    state =
      case result do
        {:ok,
         %{"route" => ^route, "identity" => identity, "response" => %{"settings" => settings}}}
        when is_map(identity) and is_map(settings) ->
          state = update_access(state, entry.client, identity, settings, entry.refresh_items)
          %{state | refresh_pending: max(0, state.refresh_pending - 1)}

        {:ok, %{"response" => response}} ->
          refresh_failed(entry, response["code"] || "QUERY_UNAVAILABLE", state)

        {:error, code} when is_binary(code) ->
          refresh_failed(entry, code, state)

        _ ->
          refresh_failed(entry, "QUERY_OUTCOME_UNKNOWN", state)
      end

    if state.refresh_pending == 0 do
      for {_pid, client} <- state.sessions,
          client.rtc and not is_nil(client.peer_id),
          do: list_peers(client, state)
    end

    {:noreply, dispatch(state)}
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
                do:
                  entry.initial
                  |> Map.merge(
                    Map.take(entry.client.identity, [
                      "groups",
                      "member_groups",
                      "write_groups",
                      "execute_groups",
                      "is_admin",
                      "group_bits",
                      "group_masks"
                    ])
                  )
                  |> Map.merge(Map.put(response, "type", "init_items")),
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

            state = %{state | revision: state_revision}

            if result["refresh_sessions"] == true do
              state = %{state | settings_revision: response["settings"]["settings_revision"]}

              if result["refresh_items"] == true do
                clients = for {_pid, client} <- state.sessions, not client.rtc, do: client
                enqueue_access_refresh(state, clients, true)
              else
                update_settings_in_place(state, response["settings"])
              end
            else
              state
            end
          end

        {:error, code} when is_binary(code) ->
          failed_query(entry, code, state)

        _ ->
          failed_query(entry, "QUERY_OUTCOME_UNKNOWN", state)
      end

    {:noreply, dispatch(state)}
  end

  def handle_info({ref, result}, %{runtime_tasks: runtime_tasks} = state)
      when is_map_key(runtime_tasks, ref) do
    Process.demonitor(ref, [:flush])
    {entry, runtime_tasks} = Map.pop!(runtime_tasks, ref)
    response = runtime_response(entry, result)
    if Map.has_key?(state.sessions, entry.client.pid), do: deliver(entry.client.pid, response)
    {:noreply, %{state | runtime_tasks: runtime_tasks}}
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
      state =
        if Map.get(entry, :access_refresh, false) do
          refresh_failed(entry, "QUERY_UNAUTHORIZED", state)
        else
          force_sync(state)
          deliver(entry.client.pid, request_error(entry.event, "QUERY_UNAUTHORIZED"))
          state
        end

      {:noreply, dispatch(state)}
    end
  end

  def handle_info({:retry_save, _}, state), do: {:noreply, state}

  def handle_info({:runtime_output, route, %{"type" => "runtime_output", "output" => output}}, state)
      when is_binary(output) do
    entry = Enum.find_value(state.runtime_tasks, fn {_ref, task} -> if task.route == route, do: task end)

    if entry && Map.has_key?(state.sessions, entry.client.pid) do
      deliver(entry.client.pid, %{
        "type" => "runtime_output",
        "request_id" => entry.event["request_id"],
        "output" => output
      })
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{runtime_tasks: runtime_tasks} = state)
      when is_map_key(runtime_tasks, ref) do
    {entry, runtime_tasks} = Map.pop!(runtime_tasks, ref)
    if Map.has_key?(state.sessions, entry.client.pid),
      do: deliver(entry.client.pid, runtime_error(entry.event, "QUERY_OUTCOME_UNKNOWN"))
    {:noreply, %{state | runtime_tasks: runtime_tasks}}
  end

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

  defp update_settings_in_place(state, settings) do
    sessions =
      Map.new(state.sessions, fn {pid, client} ->
        identity = Map.put(client.identity, "settings_revision", settings["settings_revision"])

        if not client.rtc do
          personal = Map.put(settings, "can_manage", identity["is_admin"] == true)
          deliver(pid, %{"type" => "canvas_settings_changed", "settings" => personal})
        end

        {pid, %{client | identity: identity}}
      end)

    %{state | sessions: sessions}
  end

  defp enqueue_access_refresh(state, clients, refresh_items) do
    entries =
      for client <- clients,
          do: %{
            client: client,
            event: %{"type" => "canvas_access_refresh", "request_id" => Policy.id()},
            bytes: 0,
            access_refresh: true,
            refresh_items: refresh_items
          }

    %{
      state
      | queue: :queue.join(:queue.from_list(entries), state.queue),
        refresh_pending: state.refresh_pending + length(entries)
    }
  end

  defp update_access(state, source, identity, settings, refresh_items) do
    fields =
      Map.take(identity, [
        "groups",
        "member_groups",
        "write_groups",
        "execute_groups",
        "is_admin",
        "group_bits",
        "group_masks",
        "settings_revision"
      ])

    # Both transports retain their connection IDs and expiry, while sharing the new ACL.
    sessions =
      Map.new(state.sessions, fn {pid, client} ->
        if client.identity["user_id"] == source.identity["user_id"] do
          clean = Map.drop(client.identity, ["_group_index", "_read_mask"])
          {pid, %{client | identity: Policy.compile(Map.merge(clean, fields))}}
        else
          {pid, client}
        end
      end)

    state = %{state | sessions: sessions}

    if Map.has_key?(sessions, source.pid) do
      deliver(
        source.pid,
        Map.merge(fields, %{
          "type" => "canvas_access_changed",
          "settings" => settings,
          "refresh_items" => refresh_items
        })
      )

      if refresh_items,
        do:
          deliver(source.pid, %{
            "type" => "canvas_watermark",
            "revision" => state.revision,
            "force_sync" => true
          })
    end

    state
  end

  defp refresh_failed(entry, code, state) do
    transient =
      code in ["SETTINGS_STORAGE_ERROR", "CANVAS_NOT_READY"] and save_identity_valid?(entry)

    if transient or retry_save?(entry, code) do
      defer_save(entry, code, state)
    else
      clients =
        for {pid, client} <- state.sessions,
            client.identity["user_id"] == entry.client.identity["user_id"],
            do: pid

      state =
        Enum.reduce(clients, state, fn pid, current ->
          deliver(pid, %{"type" => "canvas_access_revoked", "code" => code})
          send(pid, {:close, :revoked})
          remove(pid, current)
        end)

      %{state | refresh_pending: max(0, state.refresh_pending - 1)}
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
              Map.get(entry, :access_refresh, false) ->
            dispatch(%{state | refresh_pending: max(0, state.refresh_pending - 1)})

          not Map.has_key?(state.sessions, entry.client.pid) and
              entry.event["type"] != "canvas_ops" ->
            dispatch(state)

          true ->
            start_entry(entry, state)
        end
    end
  end

  defp start_entry(entry, state) do
    # Queued requests must use the current ACL, never the enqueue-time identity.
    entry = %{entry | client: Map.get(state.sessions, entry.client.pid, entry.client)}
    backend = Application.get_env(:agora_broker, :backend, AgoraBroker.Backend)

    task =
      try do
        {:ok,
         Task.Supervisor.async_nolink(AgoraBroker.Tasks, fn ->
           backend.query(%{
             "route" => route(entry.client, entry.event),
             "identity" => Map.drop(entry.client.identity, ["_group_index", "_read_mask"]),
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
      (entry.event["type"] == "canvas_ops" or Map.get(entry, :access_refresh, false)) and
        code in @retryable_save_errors and
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

  defp failed_query(%{access_refresh: true} = entry, code, state),
    do: refresh_failed(entry, code, state)

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
    do: Map.update(status, :state, %{}, &Map.drop(&1, [:sessions, :queue, :active, :runtime_tasks]))

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

  defp voice_public(client, id) do
    Policy.public(client.identity, id)
    |> Map.put("voice_active", Map.get(client, :voice_active, false))
    |> Map.put("voice_muted", Map.get(client, :voice_muted, false))
    |> Map.put("screen_sharing", Map.get(client, :screen_sharing, false))
  end

  defp list_peers(client, state) do
    peers =
      for {id, pid} <- state.peers,
          pid != client.pid,
          do: voice_public(state.sessions[pid], id)

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

      event |> Map.delete("deltas") |> Map.put("translations", translations)
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

      deltas = event["deltas"] || %{}
      if identity["delta_commits"] == true do
        {full, patches} = Enum.reduce(items, {%{}, %{}}, fn {id, item}, {full, patches} ->
          if not is_nil(item) and Map.has_key?(deltas, id) and Policy.visible?(identity, previous[id]) do
            {full, Map.put(patches, id, deltas[id])}
          else
            {Map.put(full, id, item), patches}
          end
        end)
        event |> Map.delete("deltas") |> Map.put("items", full) |> Map.put("patches", patches)
      else
        event |> Map.delete("deltas") |> Map.put("items", items)
      end
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
