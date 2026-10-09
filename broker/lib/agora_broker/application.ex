defmodule AgoraBroker.Application do
  use Application

  def start(_type, _args) do
    backends =
      Application.get_env(:agora_broker, :targets, %{})
      |> Enum.map(&{AgoraBroker.BackendSocket, &1})

    children = [
      {Registry, keys: :unique, name: AgoraBroker.BackendRegistry},
      {Phoenix.PubSub, name: AgoraBroker.PubSub},
      {Registry, keys: :unique, name: AgoraBroker.RoomRegistry},
      {DynamicSupervisor, name: AgoraBroker.RoomSupervisor, strategy: :one_for_one},
      {Task.Supervisor, name: AgoraBroker.Tasks, max_children: 128},
      AgoraBroker.Endpoint
    ]

    Supervisor.start_link(children ++ backends,
      strategy: :one_for_one,
      name: AgoraBroker.Supervisor
    )
  end
end
