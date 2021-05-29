defmodule ShardRegistry do
  use GenServer

  def start_link(stream_shard_list) when is_list(stream_shard_list) do
    GenServer.start_link(__MODULE__, stream_shard_list, name: ShardRegistry)
  end

  @impl true
  def init(stream_shard_list) when is_list(stream_shard_list) do
    {:ok, stream_shard_list}
  end

  @impl true
  def handle_call(:get_shard, _from, [{stream_name, shard_id} | tail]) do
    {:reply, {stream_name, shard_id}, tail}
  end

  @impl true
  def handle_call(:get_shard, _from, []) do
    {:reply, nil, []}
  end

  def get_shard() do
    GenServer.call(__MODULE__, :get_shard)
  end
end
