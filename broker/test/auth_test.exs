defmodule AgoraBroker.AuthTest do
  use ExUnit.Case, async: false
  alias AgoraBroker.{Auth, Socket, JwtFixture}

  setup do
    Application.put_env(:agora_broker, :jwt_secret, JwtFixture.secret())
    Application.put_env(:agora_broker, :server_identities, %{"8002" => "agora-cpp"})
    Application.put_env(:agora_broker, :backend, AgoraBroker.TestBackend)
    Application.put_env(:agora_broker, :test_observer, self())

    on_exit(fn ->
      Application.delete_env(:agora_broker, :test_observer)
      Application.delete_env(:agora_broker, :deny_connection)
    end)

    :ok
  end

  test "valid HS256 token binds canvas, server, IP and original identity" do
    assert {:ok, identity, expiry} =
             Auth.verify(JwtFixture.sign(JwtFixture.claims()), 1, "::ffff:127.0.0.1", 8002)

    assert identity["canvas_id"] == 1
    assert identity["nickname"] == "tester"
    assert expiry <= System.system_time(:second) + 300
  end

  test "rejects wrong signatures, algorithms, expired and missing claims" do
    claims = JwtFixture.claims()

    invalid = [
      JwtFixture.sign(claims, String.duplicate("w", 64)),
      JwtFixture.sign(claims, JwtFixture.secret(), "HS384"),
      "malformed"
    ]

    for token <- invalid,
        do: assert(Auth.verify(token, 1, "127.0.0.1", 8002) == {:error, :unauthorized})

    for key <- [
          "exp",
          "nickname",
          "tagNumber",
          "settingsRevision",
          "clientIp",
          "serverHash",
          "canvasId",
          "sub"
        ] do
      assert Auth.verify(JwtFixture.sign(Map.delete(claims, key)), 1, "127.0.0.1", 8002) ==
               {:error, :unauthorized}
    end

    for changes <- [
          %{"exp" => System.system_time(:second) - 1},
          %{"exp" => 1.5},
          %{"nbf" => System.system_time(:second) + 60},
          %{"sub" => "access"},
          %{"settingsRevision" => -1},
          %{"tagNumber" => -1}
        ] do
      assert Auth.verify(JwtFixture.sign(Map.merge(claims, changes)), 1, "127.0.0.1", 8002) ==
               {:error, :unauthorized}
    end
  end

  test "rejects canvas, IP and registered-server mismatches" do
    token = JwtFixture.sign(JwtFixture.claims())
    assert Auth.verify(token, 2, "127.0.0.1", 8002) == {:error, :unauthorized}
    assert Auth.verify(token, 1, "127.0.0.2", 8002) == {:error, :unauthorized}
    assert Auth.verify(token, 1, "127.0.0.1", 8003) == {:error, :unauthorized}
  end

  defp connect(token, canvas \\ "1") do
    Socket.connect(%{
      params: %{"canvas_id" => canvas, "ws_port" => "8002", "token" => token},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}, x_headers: []}
    })
  end

  test "invalid credentials reject before any C++ access or websocket upgrade" do
    assert connect("invalid") == :error
    refute_receive {:connect_query, _}
    assert connect(JwtFixture.sign(JwtFixture.claims()), "2") == :error
    refute_receive {:connect_query, _}
  end

  test "valid credentials are forwarded as verified claims without JWT or capability" do
    assert {:ok, state} = connect(JwtFixture.sign(JwtFixture.claims()))
    assert_receive {:connect_query, input}
    refute Map.has_key?(input, "token")
    refute Map.has_key?(state, :capability)
    assert state.identity["canvas_id"] == 1
    assert state.identity["connection_id"] == state.route["connection_id"]
    assert input["principal"]["nickname"] == "tester"
  end

  test "failed authoritative membership lookup prevents upgrade" do
    Application.put_env(:agora_broker, :deny_connection, true)
    assert connect(JwtFixture.sign(JwtFixture.claims())) == :error
  end

  test "untrusted peers cannot spoof the client IP header" do
    token = JwtFixture.sign(Map.put(JwtFixture.claims(), "clientIp", "192.0.2.1"))

    input = %{
      params: %{"canvas_id" => "1", "ws_port" => "8002", "token" => token},
      connect_info: %{
        peer_data: %{address: {127, 0, 0, 1}},
        x_headers: [{"x-real-ip", "192.0.2.1"}]
      }
    }

    Application.put_env(:agora_broker, :trusted_proxy_ips, [])
    assert Socket.connect(input) == :error
    Application.put_env(:agora_broker, :trusted_proxy_ips, ["127.0.0.1"])
    assert {:ok, _} = Socket.connect(input)
    Application.delete_env(:agora_broker, :trusted_proxy_ips)
  end
end
