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
    shard_iterator = KinesisClient.get_shard_iterator("tc-test-26May2021", "shardId-000000000000", :after_sequence_number)
    {:producer, {shard_iterator, 0}}
  end

  def handle_demand(demand, {shard_iterator, checkpoint} = state) do
    {:noreply, 0, state}
  end
end