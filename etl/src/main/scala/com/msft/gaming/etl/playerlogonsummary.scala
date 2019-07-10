package com.msft.gaming.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.datastrat.etl._

object playerlogonsummary {
  class warehouse(env: String, conf: Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, "gaming", ("warehouse", "playerlogonsummary"), Array(("stage", "activitylog"))) {
    override def extractInternal(args:Array[String]): ExtractResult = {
      import spark.implicits._

      val w = Window.partitionBy("player_id", "game_id").orderBy("activity_ts")
      val al = spark.table(tn("stage", "activitylog"))
      // month_id, player_id, logon_secs
      val df1 = al.filter('type.isin("LOGON", "LOGOFF"))
        .select('player_id, 'game_id, 'type, 'activity_ts)
        .withColumn("last_type", lag("type", 1).over(w))
        .withColumn("next_type", lead("type", 1).over(w))
        .filter(('type === "LOGON" and ('last_type.isNull or ('last_type !== "LOGON")))
          or ('type === "LOGOFF" and ('next_type.isNull or ('next_type !== "LOGOFF"))))
        .withColumn("next_ts", lead("activity_ts", 1).over(w))
        .withColumn("next_type", lead("type", 1).over(w))
        .drop("last_type")
        .filter('type === "LOGON" and ('next_type.isNull or ('next_type === "LOGOFF")))
        .cache
      val df = df1.withColumn("month_id", date_format('activity_ts, "yyyyMM"))
        .withColumn("next_month_id", date_format('next_ts, "yyyyMM"))
        
        
      df.show(50, false)

      ExtractResult(null, Some(df), "StageToCore")
    }
  }
}
