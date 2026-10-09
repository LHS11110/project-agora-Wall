defmodule AgoraBroker.Auth do
  import Bitwise

  def verify(token, canvas_id, client_ip, port)
      when is_binary(token) and byte_size(token) <= 16_384 do
    secret = Application.fetch_env!(:agora_broker, :jwt_secret)

    with true <- byte_size(secret) >= 32,
         {true, %JOSE.JWT{fields: claims}, jws} <-
           JOSE.JWT.verify_strict(JOSE.JWK.from_oct(secret), ["HS256"], token),
         false <- Map.has_key?(jws.fields, "crit"),
         true <- jws.fields["typ"] in [nil, "JWT"],
         "canvas-access" <- claims["sub"],
         ^canvas_id <- claims["canvasId"],
         exp when is_integer(exp) <- claims["exp"],
         now = System.system_time(:second),
         true <- exp > now,
         true <- valid_time?(claims["nbf"], now),
         true <- valid_time?(claims["iat"], now),
         nickname when is_binary(nickname) and byte_size(nickname) in 1..256 <-
           claims["nickname"],
         tag when is_integer(tag) and tag >= 0 and tag <= 2_147_483_647 <- claims["tagNumber"],
         revision
         when is_integer(revision) and revision >= 0 and revision <= 9_223_372_036_854_775_807 <-
           claims["settingsRevision"],
         {:ok, ip} <- normalize_ip(claims["clientIp"]),
         {:ok, ^ip} <- normalize_ip(client_ip),
         host when is_binary(host) <-
           Application.fetch_env!(:agora_broker, :server_identities)[to_string(port)],
         hash when is_binary(hash) and byte_size(hash) == 64 <- claims["serverHash"],
         expected =
           :crypto.hash(:sha256, host <> ":" <> to_string(port)) |> Base.encode16(case: :lower),
         true <- Plug.Crypto.secure_compare(hash, expected) do
      {:ok,
       %{
         "canvas_id" => canvas_id,
         "nickname" => nickname,
         "tag_number" => tag,
         "settings_revision" => revision
       }, min(exp, now + 300)}
    else
      _ -> {:error, :unauthorized}
    end
  rescue
    _ -> {:error, :unauthorized}
  catch
    _, _ -> {:error, :unauthorized}
  end

  def verify(_, _, _, _), do: {:error, :unauthorized}
  defp valid_time?(nil, _), do: true
  defp valid_time?(time, now), do: is_integer(time) and time <= now

  defp normalize_ip(ip) when is_binary(ip) do
    case :inet.parse_address(String.to_charlist(ip)) do
      {:ok, {0, 0, 0, 0, 0, 65_535, a, b}} -> {:ok, {a >>> 8, a &&& 255, b >>> 8, b &&& 255}}
      result -> result
    end
  end

  defp normalize_ip(_), do: :error
end
