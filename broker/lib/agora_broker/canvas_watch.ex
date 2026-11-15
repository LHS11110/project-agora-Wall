defmodule AgoraBroker.CanvasWatch do
  defp users(state), do: Enum.filter(state.sessions, fn {_, c} -> not c.rtc end)
  defp id(c), do: c.watch_id
  defp frame(c), do: %{"id" => id(c), "nickname" => c.identity["nickname"], "tag_number" => c.identity["tag_number"], "presenting" => Map.get(c, :presenting, false), "watching_id" => Map.get(c, :watching_id), "watch_kind" => Map.get(c, :watch_kind), "user_id" => c.identity["user_id"]}
  defp deliver(pid, event), do: send(pid, {:deliver, event})
  def sync(state) do
    people = Enum.map(users(state), fn {_, c} -> frame(c) end)
    for {pid, c} <- users(state), do: deliver(pid, %{"type" => "canvas_watch_state", "self_id" => id(c), "watching_id" => Map.get(c, :watching_id), "users" => people, "watch_kind" => Map.get(c, :watch_kind), "watch_end_reason" => Map.get(c, :watch_end_reason)})
    state
  end
  def reconcile(state) do
    ids = MapSet.new(users(state), fn {_, c} -> id(c) end)
    sessions = Map.new(state.sessions, fn {pid, c} -> {pid, if(Map.get(c, :watching_id) && not MapSet.member?(ids, c.watching_id), do: c |> Map.put(:watching_id, nil) |> Map.put(:watch_end_reason, "disconnected"), else: c)} end)
    sync(%{state | sessions: sessions})
  end
  def blocked?(client, event) do
    if Map.get(client, :watching_id) do
      case event do
        %{"type" => type} when type in ["canvas_ops", "workspace_ops", "canvas_settings_update"] -> true
        %{"type" => "runtime_execute", "action" => "agent", "operation" => op} when op in ["status", "sessions", "session_get"] -> false
        %{"type" => "runtime_execute", "action" => "service", "service" => service} when is_map(service) -> not (service["operation"] == "resource" or (service["operation"] in [nil, "api"] and Map.get(service, "method", "GET") == "GET"))
        %{"type" => "runtime_execute"} -> true
        _ -> false
      end
    else
      false
    end
  end
  defp camera?(v) when is_map(v), do: Enum.all?(["cx", "cy", "width", "height"], fn k -> is_number(v[k]) and abs(v[k]) <= 1.0e12 end) and v["width"] > 0 and v["height"] > 0 and (is_nil(v["scale"]) or (is_number(v["scale"]) and v["scale"] > 0 and v["scale"] <= 1.0e12))
  defp camera?(_), do: false
  defp put(state, c), do: %{state | sessions: Map.put(state.sessions, c.pid, c)}
  defp emit_camera(pid, source), do: if(Map.get(source, :watch_camera), do: deliver(pid, %{"type" => "canvas_watch_camera", "source_id" => id(source), "camera" => source.watch_camera}))
  def handle(client, event, state) do
    action = event["action"]
    target = Enum.find_value(users(state), fn {_, c} -> if id(c) == event["target_id"], do: c end)
    followers = Enum.any?(users(state), fn {_, c} -> Map.get(c, :watching_id) == id(client) end)
    presenter = Enum.find_value(users(state), fn {_, c} -> if Map.get(c, :presenting, false) and c.pid != client.pid, do: c end)
    cond do
      state.refresh_pending > 0 -> {:reply, {:error, "WATCH_RECONNECTING"}, state}
      action == "list" ->
        state = sync(state)
        if presenter, do: deliver(client.pid, %{"type" => "canvas_watch_invite", "presenter" => frame(presenter)})
        {:reply, :ok, state}
      action == "camera" and not camera?(event["camera"]) -> {:reply, {:error, "WATCH_CAMERA_INVALID"}, state}
      action == "camera" ->
        now = System.monotonic_time(:millisecond)
        if now - Map.get(client, :watch_sent_at, now - 100) < 60 do
          c = Map.put(client, :watch_camera, Map.take(event["camera"], ["cx", "cy", "width", "height", "scale"]))
          {:reply, :ok, put(state, c)}
        else
          c = client |> Map.put(:watch_camera, Map.take(event["camera"], ["cx", "cy", "width", "height", "scale"])) |> Map.put(:watch_sent_at, now)
          for {pid, other} <- users(state), Map.get(other, :watching_id) == id(c), do: emit_camera(pid, c)
          {:reply, :ok, put(state, c)}
        end
      action == "follow" and (is_nil(target) or target.pid == client.pid or Map.get(target, :watching_id) != nil) -> {:reply, {:error, "WATCH_TARGET_UNAVAILABLE"}, state}
      action == "follow" and (followers or Map.get(client, :presenting, false)) -> {:reply, {:error, "WATCH_HAS_FOLLOWERS"}, state}
      action == "follow" and event["mode"] == "presentation" and not Map.get(target, :presenting, false) -> {:reply, {:error, "WATCH_TARGET_UNAVAILABLE"}, state}
      action == "follow" ->
        c = client |> Map.put(:watching_id, id(target)) |> Map.put(:watch_kind, if(event["mode"] == "presentation", do: "presentation", else: "manual")) |> Map.put(:watch_end_reason, nil)
        state = sync(put(state, c));emit_camera(client.pid, target)
        deliver(target.pid, %{"type" => "canvas_watch_camera_requested"})
        {:reply, :ok, state}
      action == "leave" -> {:reply, :ok, sync(put(state, client |> Map.put(:watching_id, nil) |> Map.put(:watch_end_reason, "left")))}
      action == "start" and not is_nil(presenter) -> {:reply, {:error, "WATCH_PRESENTER_BUSY"}, state}
      action == "start" ->
        c = client |> Map.put(:presenting, true) |> Map.put(:watching_id, nil)
        if not Map.get(client, :presenting, false), do: (for {pid, _} <- users(state), pid != client.pid, do: deliver(pid, %{"type" => "canvas_watch_invite", "presenter" => frame(c)}))
        {:reply, :ok, sync(put(state, c))}
      action == "stop" ->
        sessions = Map.new(state.sessions, fn {pid, c} -> {pid, cond do
          pid == client.pid -> Map.put(c, :presenting, false)
          Map.get(c, :watching_id) == id(client) and Map.get(c, :watch_kind) == "presentation" -> c |> Map.put(:watching_id, nil) |> Map.put(:watch_end_reason, "presentation_ended")
          true -> c
        end} end)
        {:reply, :ok, sync(%{state | sessions: sessions})}
      true -> {:reply, {:error, "WATCH_REQUEST_INVALID"}, state}
    end
  end
end
