package com.msft.gaming.etl

import org.apache.spark.sql.SparkSession
import com.datastrat.etl._

object PlayerLogonSummary {
  class warehouse(env: String, conf: Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, "gaming", ("warehouse", "PlayerLogonSummary"), Array(("stage", ""))) {
  }
}
