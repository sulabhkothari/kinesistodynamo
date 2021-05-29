defmodule KinesisProducer do
  alias ExAws.Kinesis, as: KinesisClient

  require Logger
  use GenStage

  def start_link(initial \\ 0) do
    Logger.info("Starting Producer ...")
    GenStage.start_link(__MODULE__, initial, name: KinesisProducer)
  end

  def init(_) do
    {stream_name, shard_id} = ShardRegistry.get_shard()
    producer_id = UUID.uuid1()
    Logger.info("Starting Kinesis Producer  #{producer_id}")
    shard_iterator = 1
    #    shard_iterator = KinesisClient.get_shard_iterator(
    #      "tc-test-26May2021",
    #      "shardId-000000000000",
    #      :after_sequence_number
    #    )
    {:producer, {stream_name, shard_id, producer_id, 0}}
  end

  def handle_demand(demand, {stream_name, shard_id, producer_id, checkpoint} = state) do
    #    Logger.info "*******DEMAND=#{demand}**** #{
    #      state
    #      |> inspect
    #    } *******"
    demanded_data1 = wrap_records({producer_id, "#{stream_name} || #{shard_id} || #{checkpoint}", checkpoint})
    demanded_data2 = wrap_records({producer_id, "#{stream_name} || #{shard_id} || #{checkpoint + 1}", checkpoint + 1})
    {:noreply, [demanded_data1, demanded_data2], {stream_name, shard_id, producer_id, checkpoint}}
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
