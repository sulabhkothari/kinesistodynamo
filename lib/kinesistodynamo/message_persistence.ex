defmodule Message do
  @derive [ExAws.Dynamo.Encodable]
  defstruct [:user_id, :timestamp, :event]
end

defmodule MessagePersistence do
  require Logger
  def table_name(), do: "Message"

  def create_table() do
    ExAws.Dynamo.create_table(
      table_name,
      [{:user_id, :hash}, {:timestamp, :range}],
      [{:user_id, :string}, {:timestamp, :string}],
      nil,
      nil,
      :pay_per_request
    )
    |> ExAws.request
  end

  @spec update_user_message(
          message :: Message
        ) :: tuple()
  def update_user_message(%Message{user_id: user_id, timestamp: timestamp, event: event}) do
    ExAws.Dynamo.transact_write_items(
      update: {
        table_name,
        %{user_id: user_id, timestamp: timestamp},
        update_expression: "set event = :event",
        expression_attribute_values: [
          event: event
        ]
      }
    )
    |> ExAws.request
  end
end