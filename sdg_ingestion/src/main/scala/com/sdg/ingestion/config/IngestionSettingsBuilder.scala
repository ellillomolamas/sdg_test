package com.sdg.ingestion.config

import org.apache.commons.cli.ParseException

object IngestionSettingsParameters {
  val dataflowPath = "dataflowPath"
  val log4jFile = "log4j"
  val useHdfs = "useHdfs"
}


object IngestionSettingsBuilder {
  def build(args: Array[String]): IngestionSettings = {
    import scopt.OptionParser
    val parser = new OptionParser[IngestionSettings]("Ingestion Process") {
      override def showUsageOnError = true

      head("Ingestion Process", "1.0.0")

      opt[String](IngestionSettingsParameters.dataflowPath)
        .action((x, c) => c.copy(dataflowPath = x))
        .text("Dataflow path (pipeline description (in JSON))")

      opt[String](IngestionSettingsParameters.log4jFile)
        .action((x, c) => c.copy(log4jFile = x))
        .text("Log4J file to configure the logging process")

      opt[String](IngestionSettingsParameters.useHdfs)
        .action((x, c) => c.copy(useHdfs = x))
        .text("HDFS prefix (p.e 'hdfs://namenode:9000)'")

      help("help").text("prints this usage text")
    }

    parser.parse(args, IngestionSettings()) match {
      case Some(config) =>
        IngestionSettings(config.dataflowPath, config.log4jFile, config.useHdfs )
      case None =>
        throw new ParseException("Error retrieving the parameters")
    }
  }
}
