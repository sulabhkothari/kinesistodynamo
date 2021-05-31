defmodule ProducerConsumerDemandConcepts do
  @moduledoc false

end

defmodule BufferedProducer do
  use GenStage
  require Logger

  def start_link(number) do
    Logger.info "StartLink for Producer #{
      number
      |> inspect
    }"
    GenStage.start_link(__MODULE__, number, name: __MODULE__)
  end

  def init(counter) do
    Logger.info "Init for Producer"
    {:producer, counter}
  end

  def handle_info(:generate, counter) do
    events = Enum.to_list(counter..counter + 7)
    Logger.info "Asynchronously producing ==> #{events |> inspect}]"
    {:noreply, events, counter + 8} # Dispatch immediately
  end

  def handle_demand(demand, counter) when demand > 0 do
    # If the counter is 3 and we ask for 2 items, we will
    # emit the items 3 and 4, and set the state to 5.
    events = Enum.to_list(counter..counter + demand + 5)
    Process.send_after(self(), :generate, 8000)
    Logger.info "(#{counter}) Produced ==> #{
      events
      |> Enum.join(",")
    } "
    {:noreply, events, counter + demand + 6}
  end
end

defmodule BufferedConsumer do
  use GenStage
  require Logger

  def start_link(multiplier) do
    Logger.info "Consumer created"
    GenStage.start_link(__MODULE__, multiplier, name: __MODULE__)
  end

  def init(multiplier) do
    {:consumer, multiplier, subscribe_to: [{BufferedProducer, min_demand: 3, max_demand: 5}]}
  end

  def handle_events(events, _from, multiplier) do
    # events = Enum.map(events, & &1 * multiplier)
    Logger.info "(..buffering..) Consuming ==> #{
      events
      |> Enum.join(",")
    } "
    :timer.sleep(2000)
    {:noreply, [], multiplier}
  end
end
