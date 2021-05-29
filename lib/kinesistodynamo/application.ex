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

    children = [
      {ShardRegistry, [{"x", "1"}, {"y", "2"}, {"x", "2"}]},
      {KinesisConsumer, 0}
      # Starts a worker by calling: Kinesistodynamo.Worker.start_link(arg)
      # {Kinesistodynamo.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kinesistodynamo.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
