defmodule AgoraBroker.BackendSocketIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :integration
  setup do
    Application.put_env(:agora_broker, :tls_ca, System.fetch_env!("BROKER_TEST_TLS_CA"))
    Application.put_env(:agora_broker, :internal_token, "integration-test-token-32-bytes-long")
    pid = start_supervised!({AgoraBroker.BackendSocket, {"integration", "wss://localhost:9448"}})
    await_ready(pid, 100)
    %{pid: pid}
  end

  defp await_ready(_, 0), do: flunk("C++ WSS connection was not established")

  defp await_ready(pid, attempts) do
    if :sys.get_state(pid).ready do
      :ok
    else
      Process.sleep(50)
      await_ready(pid, attempts - 1)
    end
  end

  test "40 multiplexed queries reuse one TLS socket and correlate every C++ reply", %{pid: pid} do
    before = :sys.get_state(pid)

    results =
      1..40
      |> Task.async_stream(
        fn n ->
          route = %{
            "canvas_id" => 1,
            "connection_id" => "test-connection",
            "request_id" => "request-#{n}"
          }

          input = %{"command" => "query", "route" => route, "identity" => %{"canvas_id" => 2}}
          assert {:ok, result} = GenServer.call(pid, {:request, input}, 12_000)
          assert result["route"] == route
          assert result["response"]["request_id"] == route["request_id"]
          assert result["response"]["code"] == "QUERY_UNAUTHORIZED"
          n
        end,
        max_concurrency: 40,
        timeout: 15_000
      )
      |> Enum.to_list()

    assert Enum.all?(results, &match?({:ok, _}, &1))
    after_state = :sys.get_state(pid)
    assert after_state.gun == before.gun
    assert after_state.stream == before.stream
    assert after_state.pending == %{}
    assert after_state.bytes == 0
  end

  test "C++ rejects a broker with the wrong internal token" do
    {:ok, gun} =
      :gun.open(~c"localhost", 9448, %{
        transport: :tls,
        protocols: [:http],
        tls_opts: [
          versions: [:"tlsv1.3"],
          verify: :verify_peer,
          cacertfile: String.to_charlist(System.fetch_env!("BROKER_TEST_TLS_CA"))
        ]
      })

    assert {:ok, :http} = :gun.await_up(gun)
    stream = :gun.ws_upgrade(gun, "/broker/queries", [{"x-agora-internal-token", "incorrect"}])
    assert {:response, _, 401, _} = :gun.await(gun, stream)
    :gun.close(gun)
  end

  test "C++ accepts TLS 1.3 and rejects a TLS 1.2-only client" do
    options = [
      verify: :verify_peer,
      cacertfile: String.to_charlist(System.fetch_env!("BROKER_TEST_TLS_CA"))
    ]

    assert {:ok, socket} =
             :ssl.connect(~c"localhost", 9448, [versions: [:"tlsv1.3"]] ++ options, 3_000)

    assert {:ok, [{:protocol, :"tlsv1.3"}]} = :ssl.connection_information(socket, [:protocol])
    :ssl.close(socket)

    assert {:error, _} =
             :ssl.connect(~c"localhost", 9448, [versions: [:"tlsv1.2"]] ++ options, 3_000)
  end
end
