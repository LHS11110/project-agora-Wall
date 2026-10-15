defmodule AgoraBroker.Endpoint do
  use Phoenix.Endpoint, otp_app: :agora_broker

  socket("/broker", AgoraBroker.Socket,
    websocket: [
      connect_info: [:peer_data, :x_headers],
      timeout: 60_000,
      max_frame_size: 16_777_216,
      compress: false
    ],
    longpoll: false
  )

  plug(AgoraBroker.Router)
end

defmodule AgoraBroker.Errors do
  def render(_template, _assigns), do: %{error: "BROKER_ERROR"}
end
