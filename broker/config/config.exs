import Config

config :agora_broker, AgoraBroker.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [json: AgoraBroker.Errors], layout: false],
  pubsub_server: AgoraBroker.PubSub,
  server: false,
  secret_key_base: String.duplicate("test-config-only-", 8)

config :phoenix, :json_library, Jason
config :logger, level: :warning
