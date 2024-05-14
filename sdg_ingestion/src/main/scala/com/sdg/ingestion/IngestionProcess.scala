package com.sdg.ingestion

import com.sdg.ingestion.config.IngestionArgsParser
import com.sdg.ingestion.processor.Processor

object IngestionProcess {

  def main(args: Array[String]): Unit = {
    new Processor(IngestionArgsParser.parse(args)).process()
  }

}
