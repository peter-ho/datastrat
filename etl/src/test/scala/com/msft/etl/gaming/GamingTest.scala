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
  val org = "msft"
  val ara = "gaming"

/*
  "activitylog" should "load from inbound to stage" in {
    loadTbl("activitylog_in", "activitylog_in", org, ara)
    loadTbl("activitylog", "activitylog", org, ara)

    val etl = new activitylog.stage("local", org, ara, conf, spark)
    val res = etl.transform("activitylog", etl.executeInternal(Array("")))
    assert(compare("activitylog", res))
    assert(res.row_count == res.data.get.count)
  }
*/
  "playerLogonSummary" should "return the summary of player activities" in {
    loadTbl("activitylog", "activitylog", org, ara)
    loadTbl("playerlogonsummary", "playerlogonsummary", org, ara)

    val etl = new playerlogonsummary.warehouse("local", org, ara, conf, spark)
    val res = etl.transform("playerlogonsummary", etl.executeInternal(Array("")))
    assert(compare("playerlogonsummary", res))
    assert(res.row_count == res.data.get.count)
  }
}
