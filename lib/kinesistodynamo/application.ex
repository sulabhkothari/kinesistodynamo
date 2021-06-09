defmodule Kinesistodynamo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    opts = [
      stream_name: "kcl-ex-test-stream",
      app_name: "my-test-app",
      shard_consumer: KinesisConsumer,
      processors: [
        default: [
          concurrency: 1,
          min_demand: 10,
          max_demand: 20
        ]
      ],
      batchers: [
        default: [
          concurrency: 1,
          batch_size: 40
        ]
      ]
    ]
    KinesisState.create_table()
    MessagePersistence.create_table()
    children = [
      {ShardRegistry, [
        #{"alphonso1", "shardId-000000000000", 2},
        {"snowplow-enrich-good-stream", "shardId-000000000000", 1},
#        {"clixtream-enrich-good-stream", "shardId-000000000148", 5},
#        {"clixtream-enrich-good-stream", "shardId-000000000151", 5},
#        {"clixtream-enrich-good-stream", "shardId-000000000152", 5}
      ]},
      {KinesisConsumer, 0},
      #{BufferedProducer, 1},
      #{BufferedConsumer, 34},
      # Starts a worker by calling: Kinesistodynamo.Worker.start_link(arg)
      # {Kinesistodynamo.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kinesistodynamo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
