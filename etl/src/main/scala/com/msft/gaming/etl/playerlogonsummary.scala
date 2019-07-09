package com.msft.gaming.etl

import org.apache.spark.sql.SparkSession
import com.datastrat.etl._

object playerlogonsummary {
  class warehouse(env: String, conf: Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, "gaming", ("warehouse", "playerlogonsummary"), Array(("stage", "activitylog"))) {
    override def extractInternal(args:Array[String]): ExtractResult = {
      import spark.implicits._
      val df = spark.table(tn("stage", "activitylog"))
      df.show(1)
      ExtractResult(null, Some(df), "StageToCore")
    }
  }
}
