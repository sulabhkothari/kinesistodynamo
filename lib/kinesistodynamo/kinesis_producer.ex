defmodule KinesisProducer do
  alias ExAws.Kinesis, as: KinesisClient

  require Logger
  use GenStage
  import KinesisCheckpoint
  import KinesisState

  def start_link(initial \\ 0) do
    Logger.info("Starting Producer ...")
    GenStage.start_link(__MODULE__, initial, name: KinesisProducer)
  end

  defp get_shard_iterator(
         %KinesisCheckpoint{stream_name: nil, shard_id: nil, checkpoint: _},
         [stream_name: stream_name, shard_id: shard_id]
       ) do
    {_, shard_iterator} = ExAws.Kinesis.get_shard_iterator(
                            stream_name,
                            shard_id,
                            :trim_horizon
                          )
                          |> ExAws.request
    shard_iterator
  end

  defp get_shard_iterator(
         %KinesisCheckpoint{stream_name: stream_name, shard_id: shard_id, checkpoint: checkpoint},
         _
       ) do
    {_, shard_iterator} = ExAws.Kinesis.get_shard_iterator(
                            stream_name,
                            shard_id,
                            :after_sequence_number,
                            starting_sequence_number: checkpoint
                          )
                          |> ExAws.request
    shard_iterator
  end

  def init(_) do
    {stream_name, shard_id, batch_size} = ShardRegistry.get_shard()
    producer_id = UUID.uuid1()
    Logger.info("Starting Kinesis Producer  #{producer_id}")
    shard_iterator = KinesisState.get_checkpoint(stream_name: stream_name, shard_id: shard_id)
                     |> get_shard_iterator(stream_name: stream_name, shard_id: shard_id)
                     |> Map.get("ShardIterator")
    Logger.info "Shard Iterator initialized"
    {:producer, {producer_id, stream_name, shard_id, batch_size, shard_iterator, nil}}
  end

  def handle_demand(demand, {producer_id, stream_name, shard_id, batch_size, shard_iterator, _} = state) do
    {:ok, %{"NextShardIterator" => next_shard_iterator, "Records" => records}} = ExAws.Kinesis.get_records(
                                                                                   shard_iterator,
                                                                                   limit: batch_size
                                                                                 )
                                                                                 |> ExAws.request
    demanded_data = wrap_records({producer_id, records})
    {:noreply, [demanded_data], {producer_id, stream_name, shard_id, batch_size, shard_iterator, next_shard_iterator}}
  end

  @impl GenStage
  def handle_info({:ack, _ref, [], [%Broadway.Message{data: {producer_id, records}}]=failed_msgs}, state) do
    Logger.info "Messages failed #{
      records
      |> inspect
    }"
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(
        {:ack, _ref, [%Broadway.Message{data: {producer_id, records}}]=success_msgs, []},
        {producer_id, stream_name, shard_id, batch_size, shard_iterator, next_shard_iterator}
      ) do

    Logger.info "CSI: #{shard_iterator}, NCSI: #{next_shard_iterator}"
    if records != [] do
      %{"SequenceNumber" => sequence_number} = records
      |> Enum.reverse
      |> hd()
      KinesisState.update_checkpoint(%KinesisCheckpoint{stream_name: stream_name, shard_id: shard_id, checkpoint: sequence_number})
    end

    {:noreply, [], {producer_id, stream_name, shard_id, batch_size, next_shard_iterator, next_shard_iterator}}
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records(records) do
    ref = make_ref()
    import Broadway.{Message, CallerAcknowledger}
    acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
    %Broadway.Message{data: records, metadata: 0, acknowledger: acknowledger}
  end
end
