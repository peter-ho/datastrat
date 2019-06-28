package com.datastrat.util

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

class SqlExtTest extends FlatSpec {

  "replaceMaxOccuredValue" should "return a dataframe replacing a column with mostly frequently shown value given a column for partition" in {
    import spark.implicits._

    val d1 = Seq(
 ("a1", "1233","Ann","My Business")
,("a2", "1233","Ann","My Biz")
,("a3", "1233","Ana","My Business")
,("a4", "1344","Bob","Your Business")
,("a5", "2230","Cat","Car Dealership")
,("a6", "2230","Cat","Car")
,("a7", "2230","Cat","Car")
    ).toDF("TransId","PlayerId","Name","BusinessName")

    val dExpected = Seq(
 ("a1", "1233","Ann","My Business")
,("a2", "1233","Ann","My Business")
,("a3", "1233","Ana","My Business")
,("a4", "1344","Bob","Your Business")
,("a5", "2230","Cat","Car")
,("a6", "2230","Cat","Car")
,("a7", "2230","Cat","Car")
    ).toDF("TransId","PlayerId","Name","BusinessName")

    val dActual = d1.replaceMaxOccuredValue("BusinessName", Seq("PlayerId"))
    assert(compare(dExpected, dActual))
  }
}
