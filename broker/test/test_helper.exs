ExUnit.start(exclude: if(System.get_env("BROKER_TEST_TLS_CA"), do: [], else: [:integration]))
Application.put_env(:agora_broker, :targets, %{"8002" => "wss://agora-cpp:8002"})

defmodule AgoraBroker.TestBackend do
  def connect(input) do
    if pid = Application.get_env(:agora_broker, :test_observer),
      do: send(pid, {:connect_query, input})

    if Application.get_env(:agora_broker, :deny_connection), do: throw(:denied)

    identity = %{
      "canvas_id" => input["route"]["canvas_id"],
      "user_id" => 7,
      "nickname" => input["principal"]["nickname"],
      "tag_number" => input["principal"]["tag_number"],
      "groups" => ["group"],
      "is_admin" => false,
      "settings_revision" => input["principal"]["settings_revision"]
    }

    {:ok,
     %{
       "route" => input["route"],
       "identity" => identity,
       "response" => %{"type" => "init_items", "items" => %{}},
       "expires_at" => System.system_time(:second) + 60
     }}
  catch
    :denied -> {:error, "CANVAS_ACCESS_UNAUTHORIZED"}
  end

  def query(input) do
    if pid = Application.get_env(:agora_broker, :test_observer), do: send(pid, {:query, input})

    if input["event"]["backend_test_error"] do
      {:ok,
       %{
         "route" => input["route"],
         "response" => %{
           "type" => "error",
           "ok" => false,
           "code" => "ITEM_ACCESS_DENIED",
           "request_id" => input["route"]["request_id"]
         },
         "broadcasts" => []
       }}
    else
      {:ok,
       %{
         "route" => input["route"],
         "response" => %{
           "type" => "crud_result",
           "ok" => true,
           "request_id" => input["route"]["request_id"]
         },
         "broadcasts" => [%{"event" => %{"type" => "test_broadcast"}, "permission" => ["group"]}]
       }}
    end
  end
end

defmodule AgoraBroker.JwtFixture do
  def secret, do: String.duplicate("s", 64)

  def claims do
    %{
      "sub" => "canvas-access",
      "canvasId" => 1,
      "clientIp" => "127.0.0.1",
      "serverHash" => :crypto.hash(:sha256, "agora-cpp:8002") |> Base.encode16(case: :lower),
      "nickname" => "tester",
      "tagNumber" => 1,
      "settingsRevision" => 0,
      "iat" => System.system_time(:second),
      "exp" => System.system_time(:second) + 600
    }
  end

  def sign(claims, secret \\ secret(), algorithm \\ "HS256") do
    JOSE.JWT.sign(JOSE.JWK.from_oct(secret), %{"alg" => algorithm}, claims)
    |> JOSE.JWS.compact()
    |> elem(1)
  end
end
