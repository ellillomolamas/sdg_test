package com.sdg.ingestion.config

object IngestionArgsParser {

  def parse(args: Array[String]): IngestionSettings =
    IngestionSettingsBuilder.build(args: Array[String])
}
