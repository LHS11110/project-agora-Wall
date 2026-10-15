defmodule AgoraBroker.Backend do
  def connect(input), do: request("connect", input)
  def query(input), do: request("query", input)

  def request(command, input) do
    target = to_string(input["ws_port"])

    case Registry.lookup(AgoraBroker.BackendRegistry, target) do
      [{pid, _}] -> GenServer.call(pid, {:request, Map.put(input, "command", command)}, if(command == "connect", do: 45_000, else: 12_000))
      [] -> {:error, "SERVER_ROUTE_INVALID"}
    end
  catch
    :exit, _ -> {:error, "QUERY_OUTCOME_UNKNOWN"}
  end
end
