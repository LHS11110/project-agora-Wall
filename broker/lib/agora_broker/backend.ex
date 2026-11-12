defmodule AgoraBroker.Backend do
  def connect(input), do: request("connect", input)
  def query(%{"event" => %{"action" => "service"}} = input), do: AgoraBroker.CanvasService.query(input)
  def query(input), do: request("query", input)

  def request(command, input) do
    target = to_string(input["ws_port"])
    runtime_sink = Map.get(input, "runtime_sink")
    input = Map.delete(input, "runtime_sink")
    timeout =
      cond do
        command == "connect" -> 45_000
        get_in(input, ["event", "type"]) == "runtime_execute" -> 255_000
        true -> 12_000
      end

    case Registry.lookup(AgoraBroker.BackendRegistry, target) do
      [{pid, _}] -> GenServer.call(pid, {:request, Map.put(input, "command", command), runtime_sink}, timeout)
      [] -> {:error, "SERVER_ROUTE_INVALID"}
    end
  catch
    :exit, _ -> {:error, "QUERY_OUTCOME_UNKNOWN"}
  end
end
