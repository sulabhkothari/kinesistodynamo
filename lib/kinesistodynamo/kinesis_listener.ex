defmodule KinesisListener do
  @moduledoc false
  require Logger

  def create() do
    Logger.info "Fetching shard count from shard registry"
    shard_count = ShardRegistry.get_shard_count()
    Logger.info "Shard Count = #{shard_count}"
    1..shard_count
    |> Enum.each(fn _ -> Task.Supervisor.start_child(KinesisListener.Supervisor, fn -> iterate_shard() end) end)
  end

  def iterate_shard() do
    {stream_name, shard_id, batch_size} = ShardRegistry.get_shard()
    Logger.info("Starting Kinesis consumer for stream name: #{stream_name}, shard id: #{shard_id}")
    shard_iterator = KinesisState.get_checkpoint(stream_name: stream_name, shard_id: shard_id)
                     |> get_shard_iterator(stream_name: stream_name, shard_id: shard_id)
                     |> Map.get("ShardIterator")
    Logger.info "Shard Iterator initialized Stream name: #{stream_name}, shard id: #{shard_id}, batch size: #{batch_size}"
    iterate_shard(stream_name, shard_id, shard_iterator, batch_size)
  end

  def iterate_shard(stream_name, shard_id, shard_iterator, batch_size) do
    Logger.info "Reading records for Stream name: #{stream_name}, shard id: #{shard_id}, batch size: #{batch_size}"
    {next_shard_iterator, records} = ExAws.Kinesis.get_records(
                                       shard_iterator,
                                       limit: batch_size
                                     )
                                     |> ExAws.request
                                     |> process_get_records_result(shard_iterator)

    next_iterator = if records != [] do
      Logger.info "Records found !! #{shard_id}"
      MessageProcessor.process(records)
      %{"SequenceNumber" => sequence_number} = records
                                               |> Enum.reverse
                                               |> hd()
      KinesisState.update_checkpoint(
        %KinesisCheckpoint{stream_name: stream_name, shard_id: shard_id, checkpoint: sequence_number}
      )
      next_shard_iterator
    else
      Logger.info "No records found #{shard_id}"
      shard_iterator
    end
    :timer.sleep(20000)
    iterate_shard(stream_name, shard_id, next_iterator, batch_size)
  end

  def process_get_records_result({:ok, %{"NextShardIterator" => next_shard_iterator, "Records" => records}}, _) do
    Logger.info "Returning updated iterator"
    {next_shard_iterator, records}
  end

  def process_get_records_result(_, shard_iterator) do
    Logger.info "Returning current iterator"
    {shard_iterator, []}
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
end
