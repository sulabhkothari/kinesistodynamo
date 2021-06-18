defmodule MessageProcessor do
  require Logger
  def process(messages) do
    messages
    |> Enum.map(
         &(
           &1
           |> Map.get("Data")
           |> Base.decode64
           |> elem(1)
           |> TSVParser.parse)
       )
    |> Enum.each(&update/1)
  end

  def update(message) do
    Logger.info "*** Updating message #{message |> inspect} in DynamoDB ***"
    {:ok, _} = MessagePersistence.update_user_message(
      %Message{
        user_id: message
                 |> Map.get("domain_userid"),
        timestamp: message
                   |> Map.get("etl_tstamp"),
        event: message
      }
    )
    Logger.info "Update succeeded!!"
  end
end

# {\"S\":\"7117d1a0-f03c-4e83-a8a4-f690992cca54\"}
# {\"S\":\"2021-06-03 05:01:47.740\"}