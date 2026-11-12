defmodule AgoraBroker.Policy do
  import Bitwise
  def groups(permission) when is_binary(permission), do: [permission]
  def groups(permission) when is_list(permission), do: Enum.filter(permission, &is_binary/1)
  def groups(permission) when is_map(permission), do: for({name, level} <- permission, level == true or (is_number(level) and level > 0), do: name)
  def groups(_), do: []

  def compile(identity) do
    layout = identity["group_bits"]
    if is_map(layout) and map_size(layout) in 1..64 do
      slots = Map.values(layout)
      if Enum.all?(slots, &(is_integer(&1) and &1 in 0..63)) and length(Enum.uniq(slots)) == length(slots) do
        index = Map.new(layout, fn {name, slot} -> {name, 1 <<< slot} end)
        identity |> Map.put("_group_index", index) |> Map.put("_read_mask", mask(identity["groups"] || [], index))
      else
        identity
      end
    else
      identity
    end
  end

  def mask(permission, index) do
    Enum.reduce(groups(permission), 0, fn name, result -> bor(result, Map.get(index, name, 0)) end)
  end

  defp cached_mask(permission, index) do
    key = {index, permission}
    cache = Process.get({__MODULE__, :acl_masks}, %{})
    case Map.fetch(cache, key) do
      {:ok, value} -> value
      :error ->
        value = mask(permission, index)
        cache = if map_size(cache) >= 2048, do: %{}, else: cache
        Process.put({__MODULE__, :acl_masks}, Map.put(cache, key, value))
        value
    end
  end

  def visible?(%{"is_admin" => true}, _), do: true
  def visible?(%{"_group_index" => index, "_read_mask" => user_mask}, permission),
    do: band(user_mask, cached_mask(permission, index)) != 0
  def visible?(%{"group_bits" => _} = identity, permission) do
    compiled = compile(identity)
    if Map.has_key?(compiled, "_read_mask"), do: visible?(compiled, permission),
      else: Enum.any?(groups(permission), &(&1 in (identity["groups"] || [])))
  end
  def visible?(identity, permission),
    do: Enum.any?(groups(permission), &(&1 in (identity["groups"] || [])))

  def receives?(identity, %{"user_ids" => users}) when is_list(users),
    do: identity["user_id"] in users
  def receives?(identity, broadcast),
    do: broadcast["all"] == true or visible?(identity, broadcast["permission"])

  def can_execute?(%{"is_admin" => true}), do: true
  def can_execute?(identity), do: is_list(identity["execute_groups"]) and identity["execute_groups"] != []

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
      |> Map.take(["nickname", "tag_number", "groups", "write_groups", "execute_groups", "is_admin", "group_bits"])
      |> Map.put("peer_id", peer_id)

  def id, do: Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
end
