package com.msft.gaming.etl

import org.apache.spark.sql.SparkSession
import com.datastrat.etl._

object activitylog {
  class stage(env: String, conf: Map[String, String], spark:SparkSession) extends PopulateStage(env, conf, spark, "gaming", "activitylog", ("inbound", "activitylog_in")) {
  }
}
