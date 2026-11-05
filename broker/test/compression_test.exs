defmodule AgoraBroker.CompressionTest do
  use ExUnit.Case, async: true
  alias AgoraBroker.Compression
  test "only opted-in peers receive gzip; plain messages remain compatible" do
    payload=%{"type"=>"canvas_ops", "text"=>String.duplicate("공유 데이터",10000)}
    assert {:text,_}=Compression.frame(payload,%{})
    assert {:binary,bytes}=Compression.frame(payload,%{gzip: true})
    assert byte_size(bytes)<byte_size(Jason.encode!(payload))/10
    assert {:ok,decoded}=Compression.decode(bytes)
    assert Jason.decode!(decoded)==payload
    assert {:text,_}=Compression.frame(%{"type"=>"pong"},%{gzip: true})
  end
  test "decompression is bounded by decoded bytes, including highly compressible valid text" do
    text=String.duplicate("a",100000)
    assert {:ok,^text}=Compression.decode(:zlib.gzip(text),100000)
    assert {:error,:too_large}=Compression.decode(:zlib.gzip(text),99999)
    assert {:error,:invalid_gzip}=Compression.decode("bad gzip")
  end
end
