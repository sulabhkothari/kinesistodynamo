defmodule Kinesistodynamo.MixProject do
  use Mix.Project

  def project do
    [
      app: :kinesistodynamo,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Kinesistodynamo.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws_kinesis, "~> 2.0"},
      {:gen_stage, "~> 1.0.0"},
      {:jason, "~> 1.2"},
      {:hackney, "~> 1.16"},
      {:sweet_xml, "~> 0.6.6"},
      #{:kinesis_client, "~> 0.1.0"}
      #{:kinesis_client, "~> 0.2.0"},
      {:broadway, "~> 0.6.2"},
      { :uuid, "~> 1.1" }
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
