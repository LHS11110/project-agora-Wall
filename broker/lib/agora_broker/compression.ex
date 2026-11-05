defmodule AgoraBroker.Compression do
  @moduledoc "Negotiated gzip data frames with an absolute decompressed size limit."
  @limit 16_777_216
  def frame(payload, state) do
    text = Jason.encode!(payload)
    if Map.get(state, :gzip, false) and byte_size(text) >= 1024 do
      compressed = :zlib.gzip(text)
      if byte_size(compressed) < byte_size(text) * 0.9, do: {:binary, compressed}, else: {:text, text}
    else
      {:text, text}
    end
  end
  def decode(data, limit \\ @limit) do
    z = :zlib.open()
    try do
      :ok = :zlib.inflateInit(z, 31)
      case inflate(z, data, [], 0, limit) do
        {:ok, value} -> :ok = :zlib.inflateEnd(z); {:ok, value}
        error -> error
      end
    catch
      _, _ -> {:error, :invalid_gzip}
    after
      :zlib.close(z)
    end
  end
  defp inflate(z, data, chunks, size, limit) do
    {status, output} = :zlib.safeInflate(z, data)
    size = size + IO.iodata_length(output)
    cond do
      size > limit -> {:error, :too_large}
      status == :finished -> {:ok, IO.iodata_to_binary(Enum.reverse([output | chunks]))}
      true -> inflate(z, <<>>, [output | chunks], size, limit)
    end
  end
end
