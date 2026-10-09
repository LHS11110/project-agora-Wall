defmodule AgoraBroker.Router do
  use Plug.Router
  plug(:match)
  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason, length: 4096)
  plug(:dispatch)

  get "/health" do
    send_resp(conn, 200, "ok")
  end

  post "/internal/disconnect" do
    supplied = get_req_header(conn, "x-agora-internal-token") |> List.first()
    expected = Application.get_env(:agora_broker, :internal_token, "")

    if is_binary(supplied) and byte_size(expected) >= 32 and
         Plug.Crypto.secure_compare(supplied, expected) do
      id = conn.body_params["canvas_id"]
      user_id = conn.body_params["user_id"]

      if (is_nil(id) or (is_integer(id) and id > 0)) and
           (is_nil(user_id) or (is_integer(user_id) and user_id > 0)) do
        AgoraBroker.Room.control(id, conn.body_params)
        send_resp(conn, 202, "accepted")
      else
        send_resp(conn, 400, "invalid target")
      end
    else
      send_resp(conn, 401, "unauthorized")
    end
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end
