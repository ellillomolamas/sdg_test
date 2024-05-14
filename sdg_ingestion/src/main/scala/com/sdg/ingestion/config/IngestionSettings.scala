package com.sdg.ingestion.config

case class IngestionSettings(dataflowPath: String = "",
                             log4jFile: String = "",
                             useHdfs: String = "")
