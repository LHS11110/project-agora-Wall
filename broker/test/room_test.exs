defmodule AgoraBroker.RoomTest do
  use ExUnit.Case, async: false
  alias AgoraBroker.{Room, Policy}

  setup do
    Application.put_env(:agora_broker, :backend, AgoraBroker.TestBackend)
    Application.put_env(:agora_broker, :test_observer, self())
    on_exit(fn -> Application.delete_env(:agora_broker, :test_observer) end)
    canvas = :erlang.unique_integer([:positive])
    {:ok, room} = Room.ensure({8002, canvas})

    on_exit(fn ->
      if Process.alive?(room),
        do: DynamicSupervisor.terminate_child(AgoraBroker.RoomSupervisor, room)
    end)

    %{room: room, canvas: canvas}
  end

  defp client(rtc, canvas) do
    %{
      ws_port: 8002,
      route: %{"canvas_id" => canvas, "connection_id" => Policy.id(), "request_id" => Policy.id()},
      identity: %{
        "canvas_id" => canvas,
        "user_id" => 7,
        "nickname" => "a",
        "tag_number" => 1,
        "groups" => ["group"],
        "is_admin" => false
      },
      rtc: rtc,
      response: %{"type" => "init_items"}
    }
  end

  test "host election covers every group and respects highest peer ID" do
    peers = [
      %{"peer_id" => "a", "groups" => ["one"], "is_admin" => false},
      %{"peer_id" => "b", "groups" => ["one", "two"], "is_admin" => false},
      %{"peer_id" => "c", "groups" => ["two"], "is_admin" => false}
    ]

    assert Policy.elect(peers) == "b"
    assert Policy.visible?(%{"groups" => ["one"], "is_admin" => false}, ["one"])
    refute Policy.visible?(%{"groups" => ["one"], "is_admin" => false}, ["two"])
  end

  test "delegated query uses broker identity and correlates response and broadcast", %{
    room: room,
    canvas: canvas
  } do
    base = client(false, canvas)
    :ok = Room.join(room, self(), base)

    assert_receive {:deliver,
                    %{
                      "rtc_canvas_connection_id" => connection,
                      "rtc_canvas_connection_hash" => proof
                    }}

    parent = self()

    rtc =
      spawn(fn ->
        :ok = Room.join(room, self(), client(true, canvas))

        :ok =
          Room.submit(room, self(), %{
            "type" => "rtc_join",
            "host_protocol" => 1,
            "canvas_connection_id" => connection,
            "canvas_connection_hash" => proof
          })

        receive do
          {:deliver, %{"type" => "rtc_host_changed", "host" => host}} ->
            send(parent, {:host, host})
        end

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:host, host}

    :ok =
      Room.submit(room, self(), %{
        "type" => "host_item_batch",
        "host_term" => host["term"],
        "request_id" => "request-1",
        "canvas_id" => 999,
        "user_id" => 999,
        "capability" => "forged",
        "identity" => %{"canvas_id" => 999, "is_admin" => true},
        "changes" => []
      })

    assert_receive {:query, input}
    assert input["identity"] == base.identity
    refute Map.has_key?(input, "capability")
    assert input["route"]["connection_id"] == base.route["connection_id"]
    refute Map.has_key?(input["event"], "user_id")
    refute Map.has_key?(input["event"], "canvas_id")
    refute Map.has_key?(input["event"], "identity")
    assert_receive {:deliver, %{"type" => "crud_result", "request_id" => "request-1"}}
    assert_receive {:deliver, %{"type" => "test_broadcast"}}

    assert {:error, "HOST_FENCED"} =
             Room.submit(room, self(), %{"type" => "host_item_batch", "host_term" => "stale"})

    :ok =
      Room.submit(room, self(), %{
        "type" => "host_item_batch",
        "host_term" => host["term"],
        "request_id" => "denied-request",
        "changes" => [],
        "backend_test_error" => true
      })

    assert_receive {:deliver,
                    %{
                      "type" => "host_batch_result",
                      "ok" => false,
                      "code" => "ITEM_ACCESS_DENIED",
                      "request_id" => "denied-request"
                    }}

    send(rtc, :stop)
  end

  test "signaling binding rejects a forged canvas proof", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(true, canvas))

    assert {:error, "RTC_CANVAS_CONNECTION_INVALID"} =
             Room.submit(room, self(), %{
               "type" => "rtc_join",
               "host_protocol" => 1,
               "canvas_connection_id" => "forged",
               "canvas_connection_hash" => "forged"
             })
  end

  test "canvas identity cannot join another room and broadcasts stay in their canvas", %{
    room: room,
    canvas: canvas
  } do
    {:ok, other} = Room.ensure({8002, canvas + 1})

    on_exit(fn ->
      if Process.alive?(other),
        do: DynamicSupervisor.terminate_child(AgoraBroker.RoomSupervisor, other)
    end)

    base = client(false, canvas)
    assert {:error, "CANVAS_ROUTE_MISMATCH"} = Room.join(other, self(), base)
    :ok = Room.join(room, self(), base)
    assert_receive {:deliver, %{"type" => "init_items"}}
    parent = self()

    watcher =
      spawn(fn ->
        :ok = Room.join(other, self(), client(false, canvas + 1))

        receive do
          {:deliver, %{"type" => "init_items"}} -> send(parent, :other_ready)
        end

        receive do
          message -> send(parent, {:cross_canvas, message})
        end
      end)

    assert_receive :other_ready
    :ok = Room.submit(room, self(), %{"type" => "chat_history", "request_id" => "isolated"})
    assert_receive {:deliver, %{"type" => "test_broadcast"}}
    refute_receive {:cross_canvas, {:deliver, _}}, 100
    Process.exit(watcher, :kill)
  end
end
