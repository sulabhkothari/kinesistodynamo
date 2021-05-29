use Mix.Config
import Logger
Logger.info("Importing #{Mix.env()}.exs config file")
import_config "#{Mix.env()}.exs"