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
        concurrency: 3
      ],
      processors: [
        default: [
          concurrency: 3,
          min_demand: 0,
          max_demand: 1
        ]
      ],
      partition_by: &partition/1
    )
  end

  defp partition(%{data: {partition_by_id, _, _}}) do
    :erlang.phash2(partition_by_id)
  end

#  def transform(event, _opts) do
#    Logger.info("************  TR ==> #{event}**************")
#    %Message{
#      data: event,
#      acknowledger: {__MODULE__, :ack_id, :ack_data}
#    }
#  end

  @impl true
  def handle_message(_, %{data: {_, str, d}} = message, _) when rem(d,2) != 0 do
    Logger.info("************  F ==> #{str}     ######  #{d}   **************")
    :timer.sleep(3000)
    Message.failed(message, "Validation failed")
  end

  @impl true
  def handle_message(_, %{data: {_, str, d}} = message, _) do
    Logger.info("************  S ==> #{str}     ######  #{d}   **************")
    :timer.sleep(3000)
    message
  end

  @impl true
  def handle_failed(messages, _) do
    #Logger.info("************  Handling FAilure ==> #{messages |> inspect}**************")
    # :timer.sleep(10000)
    messages
  end
end
