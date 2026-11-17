defmodule AgoraBroker.MediaRelay do
  @moduledoc "Short-lived TURN credentials, issued only on authenticated RTC sessions."
  @ttl 86_400

  def config(identity, now \\ System.system_time(:second)) do
    case Application.get_env(:agora_broker, :media_relay) do
      %{secret: secret, urls: urls} when is_binary(secret) and byte_size(secret) >= 32 and is_list(urls) and urls != [] ->
        expires = now + @ttl
        # The non-secret nonce prevents usernames being reused across participants.
        nonce = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
        username = "#{expires}:#{identity["canvas_id"]}:#{nonce}"
        credential = :crypto.mac(:hmac, :sha, secret, username) |> Base.encode64()
        %{"ice_transport_policy" => "relay", "ice_servers" => [%{"urls" => urls, "username" => username, "credential" => credential}], "expires_at" => expires}
      _ ->
        %{"ice_transport_policy" => "relay", "ice_servers" => [], "expires_at" => now}
    end
  end
end
