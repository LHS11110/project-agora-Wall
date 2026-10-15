defmodule AgoraBroker.Socket do
  @behaviour Phoenix.Socket.Transport
  alias AgoraBroker.{Room, Policy, Auth}
  def child_spec(_), do: :ignore

  def connect(%{params: params, connect_info: info}) do
    with {id, ""} when id > 0 <- Integer.parse(params["canvas_id"] || ""),
         {port, ""} <- Integer.parse(params["ws_port"] || ""),
         token when is_binary(token) and byte_size(token) in 1..16_384 <- params["token"],
         true <- Map.has_key?(Application.get_env(:agora_broker, :targets, %{}), to_string(port)) do
      client_ip = client_ip(info)

      route = %{"canvas_id" => id, "connection_id" => Policy.id(), "request_id" => Policy.id()}
      backend = Application.get_env(:agora_broker, :backend, AgoraBroker.Backend)

      with {:ok, principal, expiry} <- Auth.verify(token, id, client_ip, port) do
        case backend.connect(%{
               "principal" => principal,
               "include_items" => false,
               "client_id" => params["client_id"] || "",
               "ws_port" => port,
               "client_ip" => client_ip,
               "route" => route
             }) do
          {:ok,
           %{
             "route" => ^route,
             "identity" => identity,
             "response" => response
           }} ->
            if valid_identity?(identity, principal) and expiry > System.system_time(:second) do
              {:ok,
               %{
                 route: route,
                 ws_port: port,
                 identity:
                   Map.merge(identity, %{
                     "canvas_id" => id,
                     "connection_id" => route["connection_id"],
                     "expires_at" => expiry
                   }),
                 response: response,
                 sync_client_id: params["client_id"] || Policy.id(),
                 expiry: expiry,
                 rtc: params["rtc"] == "true",
                 room: nil,
                 rate_second: 0,
                 rate_count: 0
               }}
            else
              :error
            end

          _ ->
            :error
        end
      else
        _ -> :error
      end
    else
      _ -> :error
    end
  end

  defp client_ip(info) do
    peer = info[:peer_data][:address] |> :inet.ntoa() |> to_string()
    trusted = Application.get_env(:agora_broker, :trusted_proxy_ips, [])

    if peer in trusted do
      Enum.find_value(info[:x_headers] || [], peer, fn {key, value} ->
        if key == "x-real-ip", do: value
      end)
    else
      peer
    end
  end

  defp valid_identity?(identity, principal) when is_map(identity) do
    identity["canvas_id"] == principal["canvas_id"] and
      identity["nickname"] == principal["nickname"] and
      identity["tag_number"] == principal["tag_number"] and
      identity["settings_revision"] == principal["settings_revision"] and
      is_integer(identity["user_id"]) and identity["user_id"] > 0 and
      is_list(identity["groups"]) and Enum.all?(identity["groups"], &is_binary/1) and
      is_boolean(identity["is_admin"])
  end

  defp valid_identity?(_, _), do: false

  def init(state) do
    {:ok, room} = Room.ensure({state.ws_port, state.route["canvas_id"]})
    Process.monitor(room)
    :ok = Room.join(room, self(), state)
    Process.send_after(self(), :heartbeat, 20_000)

    Process.send_after(
      self(),
      :expired,
      max(1, (state.expiry - System.system_time(:second)) * 1000)
    )

    {:ok, %{state | room: room}}
  end

  def handle_in({text, opts}, state) when is_binary(text) and byte_size(text) <= 16_777_216 do
    second = System.monotonic_time(:second)
    count = if second == state.rate_second, do: state.rate_count + 1, else: 1
    state = %{state | rate_second: second, rate_count: count}

    cond do
      opts[:opcode] != :text ->
        reply(%{"type" => "error", "code" => "UNSUPPORTED_FRAME"}, state)

      System.system_time(:second) >= state.expiry ->
        {:stop, {:shutdown, :expired}, state}

      count > 240 ->
        event =
          case Jason.decode(text) do
            {:ok, event} when is_map(event) -> event
            _ -> %{}
          end

        reply(error_payload(event, "RATE_LIMITED"), state)

      true ->
        case Jason.decode(text) do
          {:ok, event} when is_map(event) ->
            case Room.submit(state.room, self(), event) do
              :ok ->
                {:ok, state}

              {:error, code} ->
                reply(error_payload(event, code), state)
            end

          _ ->
            reply(%{"type" => "error", "code" => "INVALID_JSON"}, state)
        end
    end
  end

  def handle_in(_, state), do: reply(%{"type" => "error", "code" => "UNSUPPORTED_FRAME"}, state)
  def handle_info({:deliver, payload}, state), do: {:push, {:text, Jason.encode!(payload)}, state}

  def handle_info({:close, reason}, state)
      when reason in [:expired, :canvas_socket_closed, :settings_changed, :revoked, :room_restart],
      do: {:stop, {:shutdown, reason}, state}

  def handle_info({:close, reason}, state), do: {:stop, reason, state}

  def handle_info(:heartbeat, state) do
    Process.send_after(self(), :heartbeat, 20_000)
    {:push, {:ping, ""}, state}
  end

  def handle_info(:expired, state) do
    send(self(), {:close, :expired})

    {:push,
     {:text,
      Jason.encode!(%{
        type: "server_reconnect",
        reason: "credentials_expired",
        retry_after_ms: 100
      })}, state}
  end

  def handle_info({:DOWN, _, :process, room, _}, %{room: room} = state) do
    send(self(), {:close, :room_restart})

    {:push,
     {:text,
      Jason.encode!(%{
        type: "server_reconnect",
        reason: "broker_room_restarted",
        retry_after_ms: 500
      })}, state}
  end

  def handle_info(_, state), do: {:ok, state}

  def terminate(_reason, state) do
    if is_pid(state.room) and Process.alive?(state.room), do: Room.leave(state.room, self())
    :ok
  end

  defp error_payload(event, code) do
    result = %{
      "type" => if(event["type"] == "host_item_batch", do: "host_batch_result", else: "error"),
      "ok" => false,
      "code" => code,
      "request_id" => event["request_id"]
    }

    case event do
      %{"type" => "canvas_relay", "payload" => payload} when is_map(payload) ->
        Enum.reduce([{"term", "relay_term"}, {"type", "relay_type"}], result, fn {source, target},
                                                                                 acc ->
          value = payload[source]

          if is_binary(value) and byte_size(value) <= 128,
            do: Map.put(acc, target, value),
            else: acc
        end)

      _ ->
        result
    end
  end

  defp reply(payload, state), do: {:reply, :ok, {:text, Jason.encode!(payload)}, state}
end
