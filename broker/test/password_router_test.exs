defmodule AgoraBroker.PasswordRouterTest do
  use ExUnit.Case, async: false
  import Plug.Test
  test "password session endpoint is internal-only and returns false for absent rooms" do
    old = Application.get_env(:agora_broker, :internal_token)
    token = String.duplicate("internal-", 8)
    Application.put_env(:agora_broker, :internal_token, token)
    on_exit(fn -> Application.put_env(:agora_broker, :internal_token, old) end)
    body = Jason.encode!(%{canvas_id: 2147483000, user_id: 7, connection_id: "none", proof: "none"})
    rejected = conn(:post, "/internal/password-session", body) |> Plug.Conn.put_req_header("content-type", "application/json") |> AgoraBroker.Router.call([])
    assert rejected.status == 401
    checked = conn(:post, "/internal/password-session", body) |> Plug.Conn.put_req_header("content-type", "application/json") |> Plug.Conn.put_req_header("x-agora-internal-token", token) |> AgoraBroker.Router.call([])
    assert checked.status == 200
    assert Jason.decode!(checked.resp_body) == %{"active" => false}
  end
end
