defmodule AgoraBroker.ServerSyncBackend do
  def query(input) do
    send(Application.fetch_env!(:agora_broker, :test_observer), {:query, input})
    event = input["event"]

    if event["wait"],
      do: send(Application.fetch_env!(:agora_broker, :test_observer), {:waiting, self()})

    if event["wait"],
      do:
        (receive do
           :release -> :ok
         after
           2000 -> :ok
         end)

    if event["fail_once"] do
      case Agent.get_and_update(Application.fetch_env!(:agora_broker, :failure_counter), fn n ->
             {n, n + 1}
           end) do
        0 -> throw(:transient_failure)
        _ -> :ok
      end
    end

    response =
      if event["type"] == "canvas_sync",
        do: %{
          "type" => "canvas_snapshot",
          "sync_protocol" => 2,
          "revision" => 0,
          "accepted_count" => 0,
          "items" => %{}
        },
        else: %{"type" => "canvas_result", "ok" => true, "revision" => 1, "server_revision" => 1}

    {:ok,
     %{
       "route" => input["route"],
       "response" => Map.put(response, "request_id", input["route"]["request_id"]),
       "broadcasts" => event["test_broadcasts"] || []
     }}
  catch
    :transient_failure -> {:error, "QUERY_OUTCOME_UNKNOWN"}
  end
end

defmodule AgoraBroker.RoomTest do
  use ExUnit.Case, async: false
  alias AgoraBroker.{Room, Policy}

  setup do
    Application.put_env(:agora_broker, :backend, AgoraBroker.ServerSyncBackend)
    Application.put_env(:agora_broker, :test_observer, self())
    canvas = :erlang.unique_integer([:positive])
    {:ok, room} = Room.ensure({8002, canvas})

    on_exit(fn ->
      Application.delete_env(:agora_broker, :test_observer)
      Application.put_env(:agora_broker, :backend, AgoraBroker.TestBackend)

      if Process.alive?(room),
        do: DynamicSupervisor.terminate_child(AgoraBroker.RoomSupervisor, room)
    end)

    %{room: room, canvas: canvas}
  end

  defp client(rtc, canvas, group \\ "group") do
    %{
      ws_port: 8002,
      route: %{"canvas_id" => canvas, "connection_id" => Policy.id(), "request_id" => Policy.id()},
      identity: %{
        "canvas_id" => canvas,
        "user_id" => 7,
        "nickname" => "a",
        "tag_number" => 1,
        "groups" => [group],
        "is_admin" => false
      },
      rtc: rtc,
      response: %{"type" => "init_items", "canvas_name" => "Canvas"}
    }
  end

  defp listener(room, base) do
    parent = self()

    spawn(fn ->
      :ok = Room.join(room, self(), base)
      loop(parent)
    end)
  end

  defp loop(parent) do
    receive do
      {:deliver, frame} ->
        send(parent, {:frame, self(), frame})
        loop(parent)

      :stop ->
        :ok
    end
  end

  test "a data socket can submit edits without any RTC peers or host", %{
    room: room,
    canvas: canvas
  } do
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}

    :ok =
      Room.submit(room, self(), %{
        "type" => "canvas_ops",
        "request_id" => "one",
        "user_id" => 999,
        "identity" => %{"is_admin" => true}
      })

    assert_receive {:query, input}
    assert input["identity"]["user_id"] == 7
    refute Map.has_key?(input["event"], "identity")
    refute Map.has_key?(input["event"], "user_id")
    assert_receive {:deliver, %{"type" => "canvas_result", "ok" => true}}
    assert :sys.get_state(room).peers == %{}
  end

  test "legacy host writes and object relay frames are refused", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(false, canvas))

    assert {:error, "QUERY_TYPE_INVALID"} =
             Room.submit(room, self(), %{"type" => "host_item_batch"})

    assert {:error, "QUERY_TYPE_INVALID"} = Room.submit(room, self(), %{"type" => "crud_batch"})
  end

  test "registered sockets receive a serialized fresh snapshot", %{room: room, canvas: canvas} do
    base = Map.put(client(false, canvas), :sync_client_id, "tab-one")
    :ok = Room.join(room, self(), base)
    assert_receive {:query, %{"event" => %{"type" => "canvas_sync", "client_id" => "tab-one"}}}

    assert_receive {:deliver,
                    %{
                      "type" => "init_items",
                      "sync_protocol" => 2,
                      "revision" => 0,
                      "rtc_canvas_connection_hash" => proof
                    }}

    assert is_binary(proof)
  end

  test "commits reach every data socket with ACL filtering and eviction", %{
    room: room,
    canvas: canvas
  } do
    :ok = Room.join(room, self(), client(false, canvas))
    private = listener(room, client(false, canvas, "secret"))
    assert_receive {:frame, ^private, %{"type" => "init_items"}}
    assert_receive {:deliver, %{"type" => "init_items"}}

    commit = %{
      "type" => "canvas_commit",
      "revision" => 1,
      "items" => %{"private" => %{"kind" => "shape"}, "moved" => %{"kind" => "shape"}}
    }

    broadcast = %{
      "all" => true,
      "event" => commit,
      "permissions" => %{"private" => "secret", "moved" => "secret"},
      "previous_permissions" => %{"moved" => "group"}
    }

    :ok = Room.submit(room, self(), %{"type" => "canvas_ops", "test_broadcasts" => [broadcast]})

    assert_receive {:deliver,
                    %{
                      "type" => "canvas_commit",
                      "revision" => 1,
                      "items" => %{"moved" => nil} = items
                    }}

    refute Map.has_key?(items, "private")
    assert_receive {:frame, ^private, %{"type" => "canvas_commit", "items" => private_items}}
    assert map_size(private_items) == 2
    send(private, :stop)
  end

  test "compact translations retain only visible IDs", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}

    commit = %{
      "type" => "canvas_commit",
      "revision" => 1,
      "translations" => [%{"ids" => ["yes", "no"], "dx" => 1, "dy" => 2}]
    }

    :ok =
      Room.submit(room, self(), %{
        "type" => "canvas_ops",
        "test_broadcasts" => [
          %{
            "all" => true,
            "event" => commit,
            "permissions" => %{"yes" => "group", "no" => "private"}
          }
        ]
      })

    assert_receive {:deliver,
                    %{"type" => "canvas_commit", "translations" => [%{"ids" => ["yes"]}]}}
  end

  test "presence membership never elects a host", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(false, canvas))

    assert_receive {:deliver,
                    %{
                      "rtc_canvas_connection_id" => connection,
                      "rtc_canvas_connection_hash" => proof
                    }}

    rtc = listener(room, client(true, canvas))
    assert_receive {:frame, ^rtc, %{"type" => "rtc_ready"}}

    :ok =
      Room.submit(room, rtc, %{
        "type" => "rtc_join",
        "canvas_protocol" => 2,
        "canvas_transport" => "broker-v1",
        "canvas_connection_id" => connection,
        "canvas_connection_hash" => proof
      })

    assert_receive {:frame, ^rtc, %{"type" => "rtc_peers", "canvas_protocol" => 2} = peers}
    refute Map.has_key?(peers, "host")
    refute_receive {:frame, ^rtc, %{"type" => "rtc_host_changed"}}, 20

    assert {:error, "CANVAS_RELAY_TYPE_INVALID"} =
             Room.submit(room, rtc, %{
               "type" => "canvas_relay",
               "payload" => %{"type" => "host_commit"}
             })

    send(rtc, :stop)
  end

  test "unjoined sockets cannot relay and legacy membership is rejected", %{
    room: room,
    canvas: canvas
  } do
    rtc = listener(room, client(true, canvas))
    assert_receive {:frame, ^rtc, %{"type" => "rtc_ready"}}

    assert {:error, "RTC_SIGNAL_INVALID"} =
             Room.submit(room, rtc, %{
               "type" => "canvas_relay",
               "payload" => %{"type" => "cursor"}
             })

    assert {:error, "RTC_CANVAS_CONNECTION_INVALID"} =
             Room.submit(room, rtc, %{"type" => "rtc_join", "host_protocol" => 1})

    send(rtc, :stop)
  end

  test "revision watermarks heal the final missing commit", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(false, canvas))
    :ok = Room.submit(room, self(), %{"type" => "canvas_ops"})
    assert_receive {:deliver, %{"type" => "canvas_result"}}
    send(room, :watermark)
    assert_receive {:deliver, %{"type" => "canvas_watermark", "revision" => 1}}
  end

  test "cursor laser and selection presence relay without host approval", %{
    room: room,
    canvas: canvas
  } do
    :ok = Room.join(room, self(), client(false, canvas))

    assert_receive {:deliver,
                    %{
                      "rtc_canvas_connection_id" => connection_a,
                      "rtc_canvas_connection_hash" => proof_a
                    }}

    data_b = listener(room, client(false, canvas))

    assert_receive {:frame, ^data_b,
                    %{
                      "rtc_canvas_connection_id" => connection_b,
                      "rtc_canvas_connection_hash" => proof_b
                    }}

    rtc_a = listener(room, client(true, canvas))
    rtc_b = listener(room, client(true, canvas))
    assert_receive {:frame, ^rtc_a, %{"type" => "rtc_ready"}}
    assert_receive {:frame, ^rtc_b, %{"type" => "rtc_ready"}}

    for {pid, connection, proof} <- [
          {rtc_a, connection_a, proof_a},
          {rtc_b, connection_b, proof_b}
        ] do
      :ok =
        Room.submit(room, pid, %{
          "type" => "rtc_join",
          "canvas_protocol" => 2,
          "canvas_transport" => "broker-v1",
          "canvas_connection_id" => connection,
          "canvas_connection_hash" => proof
        })

      assert_receive {:frame, ^pid, %{"type" => "rtc_peers"}}
    end

    source_id = :sys.get_state(room).sessions[rtc_a].peer_id

    for type <- ["cursor", "laser", "editor_presence"] do
      :ok =
        Room.submit(room, rtc_a, %{
          "type" => "canvas_relay",
          "peer" => %{"nickname" => "forged"},
          "payload" => %{"type" => type, "x" => 1, "y" => 2}
        })

      assert_receive {:frame, ^rtc_b,
                      %{
                        "type" => "canvas_relay",
                        "peer" => %{"peer_id" => ^source_id, "nickname" => "a"},
                        "payload" => %{"type" => ^type}
                      }}
    end

    refute_receive {:query, _}, 20
    for pid <- [data_b, rtc_a, rtc_b], do: send(pid, :stop)
  end

  test "queued writes survive the originating user leaving", %{room: room, canvas: canvas} do
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}
    :ok = Room.submit(room, self(), %{"type" => "canvas_sync", "wait" => true})
    assert_receive {:waiting, worker}

    event = %{
      "type" => "canvas_ops",
      "request_id" => Policy.id(),
      "client_id" => "leaving",
      "request_count" => 1,
      "operations" => []
    }

    :ok = Room.submit(room, self(), event)
    Room.leave(room, self())
    send(worker, :release)
    assert_receive {:query, %{"event" => ^event}}, 1000
  end

  test "pending retries are coalesced and conflicting payloads rejected", %{
    room: room,
    canvas: canvas
  } do
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}

    event = %{
      "type" => "canvas_ops",
      "request_id" => Policy.id(),
      "client_id" => "retry",
      "request_count" => 1,
      "operations" => [],
      "wait" => true
    }

    :ok = Room.submit(room, self(), event)
    assert_receive {:query, %{"event" => ^event}}
    assert_receive {:waiting, worker}
    :ok = Room.submit(room, self(), event)

    assert {:error, "CLIENT_REQUEST_MISMATCH"} =
             Room.submit(
               room,
               self(),
               Map.put(event, "operations", [%{"action" => "delete", "id" => "x"}])
             )

    send(worker, :release)
    assert_receive {:deliver, %{"type" => "canvas_result"}}
    refute_receive {:query, %{"event" => ^event}}, 100
  end

  test "uncertain writes retry after author departure and hold the join barrier", %{
    room: room,
    canvas: canvas
  } do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    Application.put_env(:agora_broker, :failure_counter, counter)
    on_exit(fn -> Application.delete_env(:agora_broker, :failure_counter) end)
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}

    event = %{
      "type" => "canvas_ops",
      "request_id" => Policy.id(),
      "client_id" => "uncertain",
      "request_count" => 1,
      "operations" => [],
      "fail_once" => true
    }

    :ok = Room.submit(room, self(), event)
    assert_receive {:query, %{"event" => ^event}}
    assert_receive {:deliver, %{"code" => "QUERY_OUTCOME_UNKNOWN"}}
    Room.leave(room, self())
    late = listener(room, Map.put(client(false, canvas), :sync_client_id, "late"))
    refute_receive {:query, %{"event" => %{"type" => "canvas_sync"}}}, 100
    assert_receive {:query, %{"event" => ^event}}, 1500
    assert_receive {:query, %{"event" => %{"type" => "canvas_sync"}}}, 1000
    assert_receive {:frame, ^late, %{"type" => "init_items"}}, 1000
    send(late, :stop)
  end

  test "other users receive a commit only after backend save succeeds", %{
    room: room,
    canvas: canvas
  } do
    :ok = Room.join(room, self(), client(false, canvas))
    assert_receive {:deliver, %{"type" => "init_items"}}
    peer = listener(room, client(false, canvas))
    assert_receive {:frame, ^peer, %{"type" => "init_items"}}
    commit = %{"type" => "canvas_commit", "revision" => 1, "items" => %{}}

    event = %{
      "type" => "canvas_ops",
      "wait" => true,
      "test_broadcasts" => [%{"all" => true, "event" => commit}]
    }

    :ok = Room.submit(room, self(), event)
    assert_receive {:waiting, worker}
    refute_receive {:frame, ^peer, %{"type" => "canvas_commit"}}, 100
    send(worker, :release)
    assert_receive {:frame, ^peer, %{"type" => "canvas_commit", "revision" => 1}}, 1000
    send(peer, :stop)
  end
end
