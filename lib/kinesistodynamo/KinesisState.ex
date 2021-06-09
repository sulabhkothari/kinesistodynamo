defmodule KinesisCheckpoint do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:stream_name, :shard_id, :checkpoint]
end

defmodule KinesisState do
  require Logger
  def create_table() do
    Logger.info "**********HERE"
    ExAws.Dynamo.create_table(
      "KinesisCheckpoint",
      [{:stream_name, :hash}, {:shard_id, :range}],
      [{:stream_name, :string}, {:shard_id, :string}],
      nil,
      nil,
      :pay_per_request
    )
    |> ExAws.request
  end

  @spec update_checkpoint(
          kinesis_checkpoint :: KinesisCheckpoint
        ) :: tuple()
  def update_checkpoint(%KinesisCheckpoint{stream_name: stream_name, shard_id: shard_id, checkpoint: checkpoint}) do
    ExAws.Dynamo.transact_write_items(
      update: {
        "KinesisCheckpoint",
        %{stream_name: stream_name, shard_id: shard_id},
        update_expression: "set checkpoint = :checkpoint",
        expression_attribute_values: [
          checkpoint: checkpoint
        ]
      }
    )
    |> ExAws.request
  end

  @spec get_checkpoint(
          stream_name_and_shard_id :: keyword()
        ) :: KinesisCheckpoint
  def get_checkpoint([stream_name: stream_name, shard_id: shard_id]) do
    ExAws.Dynamo.get_item("KinesisCheckpoint", %{stream_name: stream_name, shard_id: shard_id})
    |> ExAws.request!
    |> ExAws.Dynamo.decode_item(as: KinesisCheckpoint)
  end
end