defmodule AgoraBroker.MentionPolicyTest do
  use ExUnit.Case, async: true
  alias AgoraBroker.Policy

  test "private notifications are delivered only to their exact user, including administrators" do
    notification = %{"user_ids" => [7], "event" => %{"type" => "mentions_changed"}}
    assert Policy.receives?(%{"user_id" => 7, "is_admin" => false}, notification)
    refute Policy.receives?(%{"user_id" => 8, "is_admin" => true}, notification)
    refute Policy.receives?(%{"user_id" => "7", "is_admin" => true}, notification)
    assert Policy.receives?(%{"groups" => ["writers"], "is_admin" => false}, %{"permission" => "writers"})
  end
end
