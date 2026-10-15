defmodule AgoraBroker.SocketFlowControlTest do
  use ExUnit.Case, async: true

  test "rate limiting correlates rejected persistence requests instead of leaving them pending" do
    state = %{expiry: System.system_time(:second) + 60, rate_second: System.monotonic_time(:second), rate_count: 240}
    event = %{"type" => "host_item_batch", "request_id" => "save-retry"}
    assert {:reply, :ok, {:text, text}, _} = AgoraBroker.Socket.handle_in({Jason.encode!(event), [opcode: :text]}, state)
    assert Jason.decode!(text) == %{"type" => "host_batch_result", "request_id" => "save-retry", "ok" => false, "code" => "RATE_LIMITED"}
  end

  test "idle connections receive a native WebSocket heartbeat" do
    assert {:push, {:ping, ""}, %{}} = AgoraBroker.Socket.handle_info(:heartbeat, %{})
  end
  test "large connection queries are admitted two at a time without closing the shared socket" do
    state = %{ready: true, pending: %{one: %{command: "connect"}, two: %{command: "connect"}}, bytes: 0}
    input = %{"command" => "connect", "route" => %{"connection_id" => "third", "request_id" => "join"}}
    assert {:reply, {:error, "BROKER_BUSY"}, ^state} = AgoraBroker.BackendSocket.handle_call({:request, input}, {self(), make_ref()}, state)
  end
  test "expected credential and companion socket shutdowns preserve reasons without abnormal crashes" do
    for reason <- [:expired, :canvas_socket_closed, :settings_changed, :revoked, :room_restart] do
      assert {:stop, {:shutdown, ^reason}, %{}} = AgoraBroker.Socket.handle_info({:close, reason}, %{})
    end
    assert {:stop, :unexpected_fault, %{}} = AgoraBroker.Socket.handle_info({:close, :unexpected_fault}, %{})
  end

  test "relay rejection echoes bounded correlation context, but never echoes the record payload" do
    state = %{expiry: System.system_time(:second) + 60, rate_second: System.monotonic_time(:second), rate_count: 240}
    event = %{"type" => "canvas_relay", "request_id" => "relay", "payload" =>
      %{"type" => "host_commit", "term" => "old-term", "records" => %{"private" => "do-not-echo"}}}
    assert {:reply, :ok, {:text, text}, _} = AgoraBroker.Socket.handle_in({Jason.encode!(event), [opcode: :text]}, state)
    result = Jason.decode!(text)
    assert result["relay_term"] == "old-term"
    assert result["relay_type"] == "host_commit"
    refute text =~ "do-not-echo"
    huge = put_in(event, ["payload", "term"], String.duplicate("x", 129))
    assert {:reply, :ok, {:text, bounded}, _} = AgoraBroker.Socket.handle_in({Jason.encode!(huge), [opcode: :text]}, state)
    refute Map.has_key?(Jason.decode!(bounded), "relay_term")
  end

end
