defmodule KinesisProducer do
  alias ExAws.Kinesis, as: KinesisClient

  require Logger
  use GenStage

  def start_link(initial \\ 0) do
    Logger.info("Starting Producer ...")
    GenStage.start_link(__MODULE__, initial, name: KinesisProducer)
  end

  def init(_) do
    Logger.info("Starting Kinesis Producer")
    shard_iterator = 1
#    shard_iterator = KinesisClient.get_shard_iterator(
#      "tc-test-26May2021",
#      "shardId-000000000000",
#      :after_sequence_number
#    )
    {:producer, {shard_iterator, 0}}
  end

  def handle_demand(demand, {shard_iterator, checkpoint} = state) do
    Logger.info "*******DEMAND=#{demand}**** #{state |> inspect} *******"
    demanded_data1 = wrap_records(checkpoint)
    demanded_data2 = wrap_records(checkpoint+1)
    {:noreply, [demanded_data1, demanded_data2], {shard_iterator, checkpoint+2}}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    Logger.info "******** Success: #{successful_msgs |> inspect} ********"
    Logger.info "******** Failed: #{failed_msgs |> inspect} ********"
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

    {:noreply, [], state}
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
