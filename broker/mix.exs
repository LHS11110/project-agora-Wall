defmodule AgoraBroker.MixProject do
  use Mix.Project

  def project do
    [
      app: :agora_broker,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: [
        {:phoenix, "~> 1.8.0"},
        {:bandit, "~> 1.7"},
        {:jason, "~> 1.4"},
        {:gun, "~> 2.7.0"},
        {:jose, "~> 1.11.12"}
      ]
    ]
  end

  def application,
    do: [mod: {AgoraBroker.Application, []}, extra_applications: [:logger, :ssl, :crypto]]
end
