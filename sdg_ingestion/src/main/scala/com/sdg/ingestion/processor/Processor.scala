package com.sdg.ingestion.processor

import com.sdg.ingestion.config.IngestionSettings
import com.sdg.ingestion.workers.json.JSONTools
import com.sdg.ingestion.workers.pipeline.ETLTool
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class Processor(config: IngestionSettings) {

  PropertyConfigurator.configure(config.log4jFile)
  private[this] val logger = Logger.getLogger(this.getClass)

  val sparkConf: SparkConf = new SparkConf().
    setAppName("SDG-Test-Ingestion").
    set("spark.sql.parquet.enableVectorizedReader", "false")

  val spark: SparkSession = SparkSession.builder().config(sparkConf).
    enableHiveSupport().
    config("hive.exec.dynamic.partition.mode", "nonstrict").
    getOrCreate()

  spark.sqlContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

  def process(): Unit = {
    logger.info("-----------------> Starting process")
    val dataFlows = JSONTools.readJSON(config.dataflowPath)
    logger.info("-----------------> Input JSON: " + JSONTools.printJSON(dataFlows) )
    ETLTool.process(spark, dataFlows, config.useHdfs);
    logger.info("-----------------> Finishing Process")
  }
}

