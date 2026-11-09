defmodule AgoraBroker.Router do
  use Plug.Router
  plug(:match)
  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason, length: 4096)
  plug(:dispatch)

  get "/health" do
    send_resp(conn, 200, "ok")
  end

  post "/internal/password-session" do
    supplied = get_req_header(conn, "x-agora-internal-token") |> List.first()
    expected = Application.get_env(:agora_broker, :internal_token, "")
    b = conn.body_params

    cond do
      not (is_binary(supplied) and byte_size(expected) >= 32 and
               Plug.Crypto.secure_compare(supplied, expected)) ->
        send_resp(conn, 401, "unauthorized")

      not (is_integer(b["canvas_id"]) and b["canvas_id"] > 0 and is_integer(b["user_id"]) and
             b["user_id"] > 0 and is_binary(b["connection_id"]) and is_binary(b["proof"]) and
               byte_size(b["proof"]) <= 256) ->
        send_resp(conn, 400, "invalid proof")

      true ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          200,
          Jason.encode!(%{
            active:
              AgoraBroker.Room.password_session(
                b["canvas_id"],
                b["user_id"],
                b["connection_id"],
                b["proof"]
              )
          })
        )
    end
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
