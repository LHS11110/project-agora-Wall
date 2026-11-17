defmodule AgoraBroker.MediaRelayTest do
  use ExUnit.Case, async: false
  alias AgoraBroker.MediaRelay
  setup do
    previous = Application.get_env(:agora_broker, :media_relay)
    on_exit(fn ->
      if previous, do: Application.put_env(:agora_broker, :media_relay, previous), else: Application.delete_env(:agora_broker, :media_relay)
    end)
    :ok
  end
  test "issues expiring REST credentials without exposing relay secret or user identity" do
    secret = String.duplicate("s", 48)
    urls = ["turns:frelog.p-e.kr:443?transport=tcp"]
    Application.put_env(:agora_broker, :media_relay, %{secret: secret, urls: urls})
    config = MediaRelay.config(%{"canvas_id" => 12, "user_id" => 55}, 1000)
    assert config["ice_transport_policy"] == "relay"
    assert config["expires_at"] == 87_400
    [server] = config["ice_servers"]
    assert String.starts_with?(server["username"], "87400:12:")
    assert server["credential"] == Base.encode64(:crypto.mac(:hmac, :sha, secret, server["username"]))
    assert server["urls"] == urls
    refute inspect(config) =~ secret
    refute config == MediaRelay.config(%{"canvas_id" => 12}, 1000)
  end
  test "missing relay configuration fails closed, without direct ICE fallback" do
    Application.delete_env(:agora_broker, :media_relay)
    assert MediaRelay.config(%{}, 1000) == %{"ice_transport_policy" => "relay", "ice_servers" => [], "expires_at" => 1000}
  end
end
