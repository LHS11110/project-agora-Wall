import Config

if config_env() != :test do
  required = fn key ->
    value = System.fetch_env!(key)
    if byte_size(value) < 32, do: raise("#{key} must be at least 32 bytes")
    value
  end

  token = required.("CPP_INTERNAL_API_TOKEN")

  if not Regex.match?(~r/^[\x21-\x7E]+$/, token),
    do: raise("CPP_INTERNAL_API_TOKEN must contain visible ASCII only")

  targets =
    Jason.decode!(System.get_env("BROKER_CPP_TARGETS", ~s({"8002":"wss://agora-cpp:8002"})))

  for {port, target} <- targets do
    uri = URI.parse(target)

    if not Regex.match?(~r/^80(?:0[2-9]|[1-9][0-9])$/, port) or uri.scheme != "wss" or
         is_nil(uri.host) or not is_nil(uri.userinfo) or not is_nil(uri.query) or
         not is_nil(uri.fragment) or uri.path not in [nil, ""],
       do: raise("BROKER_CPP_TARGETS requires logical WS ports mapped to WSS origins")
  end

  identities = System.get_env("BROKER_CPP_IDENTITIES", "{}") |> Jason.decode!()

  identities =
    Map.new(targets, fn {port, target} ->
      host = identities[port] || URI.parse(target).host
      if not is_binary(host) or host == "", do: raise("Invalid registered C++ identity")
      {port, host}
    end)

  origins =
    String.split(
      System.get_env("BROKER_ALLOWED_ORIGINS", "https://localhost:8443,https://127.0.0.1:8443"),
      ",",
      trim: true
    )

  if Enum.any?(origins, &(URI.parse(&1).scheme != "https")),
    do: raise("Only HTTPS origins are allowed")

  config :agora_broker,
    targets: targets,
    server_identities: identities,
    trusted_proxy_ips:
      String.split(System.get_env("BROKER_TRUSTED_PROXY_IPS", "172.21.0.250"), ",", trim: true),
    jwt_secret: required.("JWT_SECRET"),
    internal_token: token,
    tls_ca: System.fetch_env!("SERVICE_TLS_CA"),
    backend: AgoraBroker.Backend

  config :agora_broker, AgoraBroker.Endpoint,
    server: true,
    url: [host: System.get_env("BROKER_HOST", "agora-broker"), scheme: "https", port: 4443],
    secret_key_base: required.("BROKER_SECRET_KEY_BASE"),
    check_origin: origins,
    https: [
      ip: {0, 0, 0, 0},
      port: 4443,
      certfile: System.fetch_env!("SERVICE_TLS_CERT"),
      keyfile: System.fetch_env!("SERVICE_TLS_KEY"),
      thousand_island_options: [transport_options: [versions: [:"tlsv1.3"]]]
    ]
end
