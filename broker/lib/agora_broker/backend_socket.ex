defmodule AgoraBroker.BackendSocket do
  use GenServer

  def start_link({port, target}),
    do:
      GenServer.start_link(__MODULE__, target,
        name: {:via, Registry, {AgoraBroker.BackendRegistry, port}}
      )

  def child_spec({port, target}),
    do: %{id: {__MODULE__, port}, start: {__MODULE__, :start_link, [{port, target}]}}

  def init(target) do
    uri = URI.parse(target)
    ca = Application.fetch_env!(:agora_broker, :tls_ca) |> String.to_charlist()

    {:ok, gun} =
      :gun.open(String.to_charlist(uri.host), uri.port || 443, %{
        transport: :tls,
        protocols: [:http],
        retry: 10,
        retry_timeout: 1_000,
        tls_opts: [
          versions: [:"tlsv1.3"],
          verify: :verify_peer,
          cacertfile: ca,
          server_name_indication: String.to_charlist(uri.host),
          customize_hostname_check: [
            match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
          ]
        ]
      })

    Process.monitor(gun)
    {:ok, %{gun: gun, stream: nil, ready: false, pending: %{}, bytes: 0}}
  end

  def handle_call({:request, input}, from, %{ready: true} = state)
      when map_size(state.pending) < 64 do
    id = {input["route"]["connection_id"], input["route"]["request_id"]}
    payload = Jason.encode!(input)

    cond do
      Map.has_key?(state.pending, id) ->
        {:reply, {:error, "QUERY_ALREADY_PENDING"}, state}

      byte_size(payload) > 12_582_912 or state.bytes + byte_size(payload) > 32 * 1024 * 1024 ->
        {:reply, {:error, "BROKER_BUSY"}, state}

      true ->
        :ok = :gun.ws_send(state.gun, state.stream, {:text, payload})
        timer = Process.send_after(self(), {:timeout, id}, 10_000)

        pending =
          Map.put(state.pending, id, %{
            from: from,
            route: input["route"],
            timer: timer,
            bytes: byte_size(payload)
          })

        {:noreply, %{state | pending: pending, bytes: state.bytes + byte_size(payload)}}
    end
  end

  def handle_call({:request, _}, _, state),
    do: {:reply, {:error, "BROKER_BACKEND_UNAVAILABLE"}, state}

  def handle_info({:gun_up, gun, :http}, %{gun: gun} = state) do
    token = Application.fetch_env!(:agora_broker, :internal_token)

    stream =
      :gun.ws_upgrade(gun, "/broker/queries", [{"x-agora-internal-token", token}], %{
        compress: false,
        keepalive: 20_000,
        max_frame_size: 16 * 1024 * 1024
      })

    {:noreply, %{state | stream: stream, ready: false}}
  end

  def handle_info(
        {:gun_upgrade, gun, stream, ["websocket"], _},
        %{gun: gun, stream: stream} = state
      ),
      do: {:noreply, %{state | ready: true}}

  def handle_info({:gun_ws, gun, stream, {:text, text}}, %{gun: gun, stream: stream} = state)
      when byte_size(text) <= 16 * 1024 * 1024 do
    case Jason.decode(text) do
      {:ok, %{"route" => route} = result} ->
        id = {route["connection_id"], route["request_id"]}

        case Map.pop(state.pending, id) do
          {nil, _} ->
            {:noreply, state}

          {%{route: ^route} = entry, pending} ->
            Process.cancel_timer(entry.timer)
            GenServer.reply(entry.from, {:ok, result})
            {:noreply, %{state | pending: pending, bytes: state.bytes - entry.bytes}}

          _ ->
            {:stop, :invalid_query_route, fail_all(state)}
        end

      _ ->
        {:stop, :invalid_query_response, fail_all(state)}
    end
  end

  def handle_info({:gun_down, gun, _, _, _}, %{gun: gun} = state), do: {:noreply, fail_all(state)}

  def handle_info({:gun_ws, gun, _, :close}, %{gun: gun} = state),
    do: {:stop, :backend_closed, fail_all(state)}

  def handle_info({:gun_ws, gun, _, {:close, _, _}}, %{gun: gun} = state),
    do: {:stop, :backend_closed, fail_all(state)}

  def handle_info(
        {:gun_response, gun, stream, _, status, _},
        %{gun: gun, stream: stream} = state
      ),
      do: {:stop, {:upgrade_rejected, status}, fail_all(state)}

  def handle_info({:gun_error, gun, stream, reason}, %{gun: gun, stream: stream} = state),
    do: {:stop, {:upgrade_failed, reason}, fail_all(state)}

  def handle_info({:timeout, id}, state) do
    case Map.pop(state.pending, id) do
      {nil, _} ->
        {:noreply, state}

      {entry, pending} ->
        GenServer.reply(entry.from, {:error, "QUERY_OUTCOME_UNKNOWN"})
        {:noreply, %{state | pending: pending, bytes: state.bytes - entry.bytes}}
    end
  end

  def handle_info({:DOWN, _, :process, gun, reason}, %{gun: gun} = state),
    do: {:stop, {:backend_down, reason}, fail_all(state)}

  def handle_info(_, state), do: {:noreply, state}

  def terminate(_, state) do
    fail_all(state)
    :gun.close(state.gun)
  end

  def format_status(status), do: Map.update(status, :state, %{}, &Map.drop(&1, [:pending]))

  defp fail_all(state) do
    for {_, entry} <- state.pending do
      Process.cancel_timer(entry.timer)
      GenServer.reply(entry.from, {:error, "QUERY_OUTCOME_UNKNOWN"})
    end

    %{state | pending: %{}, bytes: 0, ready: false}
  end
end
