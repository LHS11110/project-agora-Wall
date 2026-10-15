defmodule AgoraBroker.SocketUpgradeIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  alias AgoraBroker.JwtFixture

  setup do
    Application.put_env(:agora_broker, :jwt_secret, JwtFixture.secret())
    Application.put_env(:agora_broker, :server_identities, %{"8002" => "agora-cpp"})
    Application.put_env(:agora_broker, :backend, AgoraBroker.TestBackend)
    Application.put_env(:agora_broker, :trusted_proxy_ips, [])
    tls = Path.dirname(System.fetch_env!("BROKER_TEST_TLS_CA"))

    pid =
      start_supervised!(
        {Bandit,
         plug: AgoraBroker.Endpoint,
         scheme: :https,
         ip: {127, 0, 0, 1},
         port: 0,
         certfile: Path.join(tls, "fullchain.pem"),
         keyfile: Path.join(tls, "privkey.pem"),
         thousand_island_options: [transport_options: [versions: [:"tlsv1.3"]]]}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(pid)

    {:ok, gun} =
      :gun.open(~c"localhost", port, %{
        transport: :tls,
        protocols: [:http],
        tls_opts: [
          versions: [:"tlsv1.3"],
          verify: :verify_peer,
          cacertfile: String.to_charlist(System.fetch_env!("BROKER_TEST_TLS_CA"))
        ]
      })

    assert {:ok, :http} = :gun.await_up(gun)

    on_exit(fn ->
      :gun.close(gun)
      Application.delete_env(:agora_broker, :deny_connection)
    end)

    %{gun: gun, port: port}
  end

  test "valid JWT receives a real WebSocket upgrade and private canvas snapshot", %{gun: gun} do
    token = JwtFixture.sign(JwtFixture.claims())
    stream = :gun.ws_upgrade(gun, "/broker/websocket?canvas_id=1&ws_port=8002&token=" <> token)
    assert_receive {:gun_upgrade, ^gun, ^stream, ["websocket"], _}, 3_000
    assert_receive {:gun_ws, ^gun, ^stream, {:text, payload}}, 3_000
    result = Jason.decode!(payload)
    assert result["type"] == "init_items"
    refute Map.has_key?(result, "identity")
    refute Map.has_key?(result, "user_id")
  end

  test "invalid signature is rejected before upgrade", %{gun: gun} do
    token = JwtFixture.sign(JwtFixture.claims(), String.duplicate("wrong", 16))
    stream = :gun.ws_upgrade(gun, "/broker/websocket?canvas_id=1&ws_port=8002&token=" <> token)
    assert {:response, _, status, _} = :gun.await(gun, stream)
    assert status in [401, 403]
    refute_receive {:gun_upgrade, ^gun, ^stream, _, _}
  end

  test "valid signature with wrong canvas or revoked membership cannot upgrade", %{gun: gun} do
    token = JwtFixture.sign(JwtFixture.claims())
    stream = :gun.ws_upgrade(gun, "/broker/websocket?canvas_id=2&ws_port=8002&token=" <> token)
    assert {:response, _, status, _} = :gun.await(gun, stream)
    assert status in [401, 403]
    Application.put_env(:agora_broker, :deny_connection, true)
    stream = :gun.ws_upgrade(gun, "/broker/websocket?canvas_id=1&ws_port=8002&token=" <> token)
    assert {:response, _, status, _} = :gun.await(gun, stream)
    assert status in [401, 403]
  end

  test "broker rejects TLS 1.2 before WebSocket upgrade", %{port: port} do
    assert {:error, _} =
             :ssl.connect(
               ~c"localhost",
               port,
               [
                 versions: [:"tlsv1.2"],
                 verify: :verify_peer,
                 cacertfile: String.to_charlist(System.fetch_env!("BROKER_TEST_TLS_CA"))
               ],
               3_000
             )
  end
  test "expected companion shutdown closes a real WebSocket without crash logs", %{gun: gun} do
    token = JwtFixture.sign(JwtFixture.claims())
    stream = :gun.ws_upgrade(gun, "/broker/websocket?canvas_id=1&ws_port=8002&token=" <> token)
    assert_receive {:gun_upgrade, ^gun, ^stream, ["websocket"], _}, 3_000
    assert_receive {:gun_ws, ^gun, ^stream, {:text, payload}}, 3_000
    connection = Jason.decode!(payload)["rtc_canvas_connection_id"]
    {:ok, room} = AgoraBroker.Room.ensure({8002, 1})
    {pid, _client} = Enum.find(:sys.get_state(room).sessions, fn {_pid, client} -> client.route["connection_id"] == connection end)
    log = ExUnit.CaptureLog.capture_log(fn ->
      send(pid, {:close, :canvas_socket_closed})
      assert_receive {:gun_ws, ^gun, ^stream, close}, 3_000
      assert is_tuple(close) and elem(close, 0) == :close
    end)
    refute log =~ "terminating"
    refute log =~ "[error]"
  end

end
