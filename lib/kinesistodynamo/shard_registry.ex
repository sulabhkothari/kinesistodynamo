defmodule ShardRegistry do
  use GenServer
  require Logger

  def start_link(stream_shard_list) do
    GenServer.start_link(__MODULE__, stream_shard_list, name: ShardRegistry)
  end

  @impl true
  def init({stream_name, batch_size}) do
    shard_list = get_shards_from_stream(stream_name) |> Enum.map(&({stream_name, &1, batch_size}))
    IO.puts "#{shard_list |> inspect}"
    Process.send_after(self(), :poll_kinesis_for_shards, 8000)
    {:ok, {shard_list, %{}}}
  end

  @impl true
  def handle_info(:kill_broadway, state) do
    GenServer.stop(KinesisConsumer, :normal)
    {:noreply, state}
  end

  @impl true
  def handle_info(:poll_kinesis_for_shards, {[], producer_registry}) do
    first_shard_id = producer_registry |> Map.keys |> List.first
    {stream_name, shard_id, batch_size} = producer_registry |> Map.get(first_shard_id)
    new_shard_list = get_shards_from_stream(stream_name) |> Enum.map(&({stream_name, &1, batch_size}))
    Logger.info "New SHARD LIST ===> #{new_shard_list |> inspect}"
    new_shard_count = new_shard_list |> Enum.count
    curr_shard_count = (producer_registry |> Map.keys |> Enum.count)
    if(new_shard_count != curr_shard_count) do
      Logger.info "Stream has been resharded from #{curr_shard_count} to #{new_shard_count} shards"
      # GenServer.stop(KinesisConsumer, :normal)
      Process.send_after(self(), :kill_broadway, 500)
      Logger.info "Spawned Broadway Killer"
      Process.send_after(self(), :poll_kinesis_for_shards, 8000)
      {:noreply, {new_shard_list, %{}}}
    else
      Logger.info "Stream is not resharded"
      Process.send_after(self(), :poll_kinesis_for_shards, 8000)
      {:noreply, {[], producer_registry}}
    end
  end

  @impl true
  def handle_info(:poll_kinesis_for_shards, state) do
    Process.send_after(self(), :poll_kinesis_for_shards, 8000)
    {:noreply, state}
  end

  def get_shards_from_stream(stream_name) when is_binary(stream_name) do
    %{"StreamDescription" => %{"Shards" => shards}} = ExAws.Kinesis.describe_stream(stream_name) |> ExAws.request!
    {shard_id_list, parent_shard_id_set} = get_shards_from_stream(shards)
    shard_id_list |> Enum.filter(&(!MapSet.member?(parent_shard_id_set, &1)))
  end

  def get_shards_from_stream([%{"ShardId" => shard_id, "ParentShardId" => parent_shard_id, "AdjacentParentShardId" => adjacent_parent_shard_id} | tail]) do
    {shard_id_list, parent_shard_id_set} = get_shards_from_stream(tail)
    {[shard_id | shard_id_list], parent_shard_id_set |> MapSet.put(parent_shard_id) |> MapSet.put(adjacent_parent_shard_id)}
  end

  def get_shards_from_stream([%{"ShardId" => shard_id, "ParentShardId" => parent_shard_id} | tail]) do
    {shard_id_list, parent_shard_id_set} = get_shards_from_stream(tail)
    {[shard_id | shard_id_list], parent_shard_id_set |> MapSet.put(parent_shard_id) }
  end

  def get_shards_from_stream([%{"ShardId" => shard_id} | tail]) do
    {shard_id_list, parent_shard_id_set} = get_shards_from_stream(tail)
    {[shard_id | shard_id_list], parent_shard_id_set}
  end

  def get_shards_from_stream([]) do
    {[], MapSet.new()}
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
  def handle_call(:get_shard_count, _from, {shard_list, _}=state) do
    shard_count = Enum.count(shard_list)
    Logger.info "Shard Count has been set to #{shard_count}"
    {:reply, shard_count, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _}, {shard_list, %{}}=state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _}, {shard_list, producer_registry}) do
    shard_info = producer_registry |> Map.get(pid)
    {:noreply, {[shard_info | shard_list], producer_registry |> Map.delete(pid)}}
  end

  def get_shard() do
    GenServer.call(__MODULE__, {:get_shard, self()})
  end

  def get_shard_count() do
    GenServer.call(__MODULE__, :get_shard_count)
  end
end
