defmodule ShardRegistry do
  use GenServer
  require Logger

  def start_link(stream_shard_list) when is_list(stream_shard_list) do
    GenServer.start_link(__MODULE__, stream_shard_list, name: ShardRegistry)
  end

  @impl true
  def init(stream_shard_list) when is_list(stream_shard_list) do
    {:ok, {stream_shard_list, %{}}}
  end

  @impl true
  def handle_call(
        {:get_shard, producer_ref},
        _from,
        {[{stream_name, shard_id, batch_size} | tail], producer_registry}
      ) do
    producer_registry = Map.put(producer_registry, producer_ref, {stream_name, shard_id, batch_size})
    Process.monitor(producer_ref)
    {:reply, {stream_name, shard_id, batch_size}, {tail, producer_registry}}
  end

  @impl true
  def handle_call({:get_shard, producer_ref}, _from, {[], pr}) do
    {:reply, nil, {[], pr}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _}, {shard_list, producer_registry}) do
    shard_info = producer_registry |> Map.get(pid)
    {:noreply, {[shard_info | shard_list], producer_registry |> Map.delete(pid)}}
  end

  def get_shard() do
    GenServer.call(__MODULE__, {:get_shard, self()})
  end
end
