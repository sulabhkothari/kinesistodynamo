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
                            starting_sequence_number: "1231"
                          )
                          |> ExAws.request
    shard_iterator
  end

  def init(_) do
    {stream_name, shard_id} = ShardRegistry.get_shard()
    producer_id = UUID.uuid1()
    Logger.info("Starting Kinesis Producer  #{producer_id}")
    shard_iterator = KinesisState.get_checkpoint(stream_name: stream_name, shard_id: shard_id)
                     |> get_shard_iterator(stream_name: stream_name, shard_id: shard_id)
                     |> Map.get("ShardIterator")
    Logger.info "Shard Iterator initialized"
    {:producer, {producer_id, shard_iterator}}
  end

  def handle_demand(demand, {producer_id, shard_iterator} = state) do
    #    Logger.info "*******DEMAND=#{demand}**** #{
    #      state
    #      |> inspect
    #    } *******"
    {:ok, %{"NextShardIterator" => next_shard_iterator, "Records" => records}} = ExAws.Kinesis.get_records(
      shard_iterator,
      limit: 1
    ) |> ExAws.request
    Logger.info "****** #{records |> hd |> Map.get("Data") |> Base.decode64 |> elem(1)} *******"
    demanded_data = wrap_records({producer_id, "#{producer_id}"})
    {:noreply, [demanded_data], {producer_id, shard_iterator}}
  end

  @impl GenStage
  def handle_info({:ack, _ref, [], failed_msgs}, state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(
        {:ack, _ref, [%{data: {_, _, msg_checkpoint}} = successful_msgs], []},
        {stream_name, shard_id, producer_id, checkpoint}
      ) do
    Logger.info "******** Success: #{
      successful_msgs.data
      |> inspect
    } ********   Failed: [] +++++++++++"
    #    %{
    #      metadata: %{
    #        "SequenceNumber" => checkpoint
    #      }
    #    } = successful_msgs
    #        |> Enum.reverse()
    #        |> hd()

    # notify({:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: []}}, state)

    #    Logger.debug(
    #      "Acknowledged #{length(successful_msgs)} messages: [app_name: #{state.app_name} " <>
    #      "shard_id: #{state.shard_id}"
    #    )

    # state = handle_closed_shard(state)

    checkpoint = if(checkpoint == msg_checkpoint) do
      Logger.info "***********PROPAGATING*****************"
      checkpoint + 1
    else
      Logger.info "***********NONPROPAGATING*****************"
      checkpoint
    end

    {:noreply, [], {stream_name, shard_id, producer_id, checkpoint}}
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records(records) do
    ref = make_ref()

    #    Enum.map(
    #      records,
    #      fn %{"Data" => data} = record ->
    #        metadata = Map.delete(record, "Data")
    #        acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
    #        %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
    #      end
    #    )
    import Broadway.{Message, CallerAcknowledger}
    acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
    %Broadway.Message{data: records, metadata: 0, acknowledger: acknowledger}
  end
end
