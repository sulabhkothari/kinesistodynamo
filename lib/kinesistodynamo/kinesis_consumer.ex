defmodule KinesisConsumer do
  require Logger
  use Broadway
  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(
      KinesisConsumer,
      name: KinesisConsumer,
      producer: [
        module: {KinesisProducer, []},
        #transformer: {__MODULE__, :transform, []},
        concurrency: ShardRegistry.get_shard_count
      ],
      processors: [
        default: [
          concurrency: ShardRegistry.get_shard_count,
          min_demand: 0,
          max_demand: 1
        ]
      ],
      partition_by: &partition/1
    )
  end

  defp partition(%{data: {partition_by_id, _}}) do
    :erlang.phash2(partition_by_id)
  end

  @impl true
  def handle_message(_, %{data: {producer_id, []}} = message, _) do
    Logger.info "No records found"
    :timer.sleep(3000)
    message
    #    Message.failed(message, "Validation failed")
  end

  @impl true
  def handle_message(_, %{data: {_, records}} = message, _) do
    Logger.info "************ SUCCESSFUL MESSAGE ========>"
    # str = records |> Enum.map(&(&1 |> Map.get("Data") |> Base.decode64 |> elem(1))) |> Enum.join(".......")
    # str |> String.split("\t") |> Enum.each(&IO.puts/1)
    # Logger.info "*********===========++++++++++++ #{str |> String.split("\t") |> Enum.count}+++++++++++++++++++++"
    # TSVParser.parse(str) |> IO.inspect
    MessageProcessor.process(records)
    :timer.sleep(10000)

    message
    #    Message.failed(message, "Validation failed")
  end
end
