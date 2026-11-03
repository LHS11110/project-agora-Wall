defmodule AgoraBroker.GroupPermissionsTest do
  use ExUnit.Case, async: true
  alias AgoraBroker.Policy
  test "write and execute groups never expand read visibility" do
    identity = %{"groups" => ["readers"], "write_groups" => ["writers"], "execute_groups" => ["operators"], "is_admin" => false, "user_id" => 42}
    assert Policy.visible?(identity, "readers")
    refute Policy.visible?(identity, "writers")
    refute Policy.visible?(identity, "operators")
    public = Policy.public(identity, "peer")
    assert public["write_groups"] == ["writers"]
    assert public["execute_groups"] == ["operators"]
    refute Map.has_key?(public, "user_id")
  end
end

defmodule AgoraBroker.GroupBitsTest do
 use ExUnit.Case, async: true
 alias AgoraBroker.Policy
 test "64th group retains full precision and disjoint roles are denied" do
  layout=Map.new(0..63, fn i -> {"g#{i}",i} end)
  identity=Policy.compile(%{"group_bits"=>layout,"groups"=>["g63"],"write_groups"=>["g0"],"is_admin"=>false})
  assert identity["_read_mask"]==9_223_372_036_854_775_808
  assert Policy.visible?(identity,["g63"])
  refute Policy.visible?(identity,["g0"])
  refute Policy.visible?(identity,["missing"])
  refute Policy.visible?(identity,%{"g63"=>false})
  assert Policy.visible?(identity,%{"g63"=>true})
  refute Map.has_key?(Policy.public(identity,"peer"),"_read_mask")
 end
 test "role changes compile a new mask and legacy identities are supported" do
  identity=%{"group_bits"=>%{"a"=>0,"b"=>63},"groups"=>["a"],"is_admin"=>false}
  assert Policy.visible?(Policy.compile(identity),"a")
  refute Policy.visible?(Policy.compile(Map.put(identity,"groups",["b"])),"a")
  assert Policy.visible?(%{"groups"=>["a"]},"a")
 end
end
