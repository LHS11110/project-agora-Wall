defmodule AgoraBroker.Policy do
  def groups(permission) when is_binary(permission), do: [permission]
  def groups(permission) when is_list(permission), do: Enum.filter(permission, &is_binary/1)
  def groups(permission) when is_map(permission), do: Map.keys(permission)
  def groups(_), do: []

  def visible?(identity, permission) do
    identity["is_admin"] == true or Enum.any?(groups(permission), &(&1 in identity["groups"]))
  end

  def receives?(identity, %{"user_ids" => users}) when is_list(users),
    do: identity["user_id"] in users
  def receives?(identity, broadcast),
    do: broadcast["all"] == true or visible?(identity, broadcast["permission"])

  def elect(peers) do
    groups = peers |> Enum.flat_map(& &1["groups"]) |> Enum.uniq()

    peers
    |> Enum.filter(&(&1["is_admin"] or Enum.all?(groups, fn group -> group in &1["groups"] end)))
    |> Enum.map(& &1["peer_id"])
    |> Enum.sort()
    |> List.last() || ""
  end

  def public(identity, peer_id),
    do:
      identity
      |> Map.take(["nickname", "tag_number", "groups", "is_admin"])
      |> Map.put("peer_id", peer_id)

  def id, do: Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
end
