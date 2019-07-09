package com.msft.etl.gaming

import java.sql.Timestamp
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest._
import com.datastrat.etl.ETLTestBase._
import com.datastrat.util.SqlExt._
import com.msft.gaming.etl._

class GamingTest extends FlatSpec {

  "activitylog" should "load from inbound to stage" in {
    loadTbl("activitylog_in", "activitylog_in", "msft", "gaming")
    loadTbl("activitylog", "activitylog", "msft", "gaming")

    val etl = new activitylog.stage("local", conf, spark)
    val res = etl.transform("activitylog", etl.extractInternal(Array("")))
    assert(compare("activitylog", res))
    assert(res.row_count == res.data.get.count)
  }

  "playerLogonSummary" should "return the summary of player activities" in {
    loadTbl("activitylog", "activitylog", "msft", "gaming")
    loadTbl("playerlogonsummary", "playerlogonsummary", "msft", "gaming")

    val etl = new playerlogonsummary.warehouse("local", conf, spark)
    val res = etl.transform("activitylog", etl.extractInternal(Array("")))
    assert(compare("playerlogonsummary", res))
    assert(res.row_count == res.data.get.count)
  }
}
