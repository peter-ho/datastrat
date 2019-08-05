package com.msft.gaming.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.datastrat.etl._

object playerlogonsummary {
  class warehouse(env:String, conf:Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, "gaming", ("warehouse", "playerlogonsummary"), Array(("stage", "activitylog"))) {
    override def executeInternal(args:Array[String]): ExeResult = {
      import spark.implicits._

      // logon counts are partitioned by each player playing each game
      val w = Window.partitionBy("player_id", "game_id", "month_id").orderBy("activity_ts")
      val al = spark.table(tn("stage", "activitylog"))

      // remove duplciates, structure one row per session for each month, each player, each game
      val df1 = al.filter('type.isin("LOGON", "LOGOFF"))    // filter only LOGON and LOGOFF types
        .withColumn("month_id", date_format('activity_ts, "yyyyMM"))
        .select('month_id, 'player_id, 'game_id, 'type, 'activity_ts)  // select only columns reqired for this job
        .withColumn("last_type", lag("type", 1).over(w))    // identify the previous type for a given player and game
        .withColumn("next_type", lead("type", 1).over(w))   // identify the next type for a given player and game
        .filter(('type === "LOGON" and ('last_type.isNull or ('last_type !== "LOGON")))   // remove consecutive LOGON 
          or ('type === "LOGOFF" and ('next_type.isNull or ('next_type !== "LOGOFF"))))   // remove consecutive LOGOFF
        .withColumn("next_type", lead("type", 1).over(w))                                 // assign type of next activity to next_type 
        .filter('type === "LOGON" and ('next_type.isNull or ('next_type === "LOGOFF"))    // select LOGON then LOGOFF or null activities
          or ('type === "LOGOFF" and 'last_type.isNull))                                  // select LOGOFF with null as previous activity
        .drop("last_type")
        .withColumn("next_ts", lead("activity_ts", 1).over(w))                            // assign next activity ts to next_ts

      df1.show

      // assign next_ts as beginning of the month if type is LOGOFF
      val df2 = df1.withColumn("next_ts", when('type === "LOGOFF", date_add(add_months(last_day('activity_ts), -1), 1))
          // assign next_ts as end of the month if not populated
          .when('next_ts.isNull, date_format(last_day('next_ts), "yyyy-MM-dd 23:59:59.999999").cast("timestamp"))
          .otherwise('next_ts))
        .withColumn("logon_secs", abs(datediff('next_ts, 'activity_ts)*24*60*60))
      df2.show

      val df = df2
        .groupBy("month_id", "player_id").agg(sum('logon_secs).as("logon_secs"))

      ExeResult(null, Some(df), "StageToCore")
    }
  }
}
