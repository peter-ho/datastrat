package com.datastrat.util

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
import com.datastrat.test.ETLTestBase._
import com.datastrat.util.SqlExt._

class GamingTest extends FlatSpec {

  "playerLogonSummary" should "return the summary of player activities" in {
    import spark.implicits._
/*
    val da = spark.table(

    val d1 = Seq(
 ("a1", new Timestamp(2019-1900,4-1, 1,0,0,0,0), new Timestamp(2019-1900,8-1, 10,0,0,0,0))
,("a2", new Timestamp(2019-1900,4-1, 1,0,0,0,0), new Timestamp(2020-1900,2-1, 10,0,0,0,0))
,("a3", new Timestamp(2020-1900,4-1, 1,0,0,0,0), new Timestamp(2019-1900,2-1, 10,0,0,0,0))
    ).toDF("TransId","StartTime","EndTime")

    val dExpected = Seq(
 ("a1", Array("201904","201905","201906","201907","201908"))
,("a2", Array("201904","201905","201906","201907","201908","201909","201910","201911","201912","202001","202002"))
,("a3", Array[String]())
    ).toDF("TransId","YearMths")

    val dActual = d1.withColumn("YearMths", udfMnthGen('StartTime,'EndTime)).drop("StartTime", "EndTime")
    assert(compare(dExpected, dActual))
*/
  }
}
