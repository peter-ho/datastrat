/*
 * Copyright (c) 2019, All rights reserved.
 *
 */
package com.datastrat.etl

import sys.process._
import java.util.{Date, Calendar, Properties}
import java.sql.{ Connection, DriverManager, Timestamp }  
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import scala.collection.mutable.{ListBuffer, ListMap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame, SaveMode, SparkSession, Dataset, Column}
import org.apache.spark.sql.expressions.{WindowSpec, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.commons.cli.MissingArgumentException
import org.apache.commons.lang3.RandomUtils
import org.apache.log4j.Logger

/**
 * @author Peter Ho
 */
class LoadHdr(loadNbr: String, numOfMnth: Int = 38) {
}


  lazy val udfMnthGen = udf((strt:Timestamp, end:Timestamp) => (0 to (12*(end.getYear - strt.getYear)+end.getMonth-strt.getMonth) toArray).map(x => ((1900 + strt.getYear + (x+strt.getMonth)/12) * 100 + (x+strt.getMonth)%12+1).toString))

  def fillExcpMsg(sb:StringBuilder, e:Throwable):Unit = {
    sb.append(e.toString).append(e.getStackTraceString).append("\n")
    if (e.getCause != null) fillExcpMsg(sb, e.getCause)
  }

  /** log execution to audit_log table with the given information about the load    *
    * @example 1
    *          {{{logExecution(AuditLog("201810161421323232", Array("source1, src2"), "target1", start, end, 232100313, 23213111, "LoadToCore")}}}
    * @param audit [[com.datastrat.etl.AuditLog]] AuditLog instance to be inserted in audit_log table
    */
  def logExecution(audit: AuditLog) : AuditLog = {
    import sess.implicits._
    sess.createDataset(List(audit)).write.mode(SaveMode.Append).parquet(s"${locations("core")}/audit_log")
    audit
  }

object Session {
  lazy val sdfConcat = new SimpleDateFormat("yyyyMMddHHmmss")
  lazy val sdfDisplay = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  lazy val sdfYrMnth = new SimpleDateFormat("yyyyMM")

  /// timestamp to be identified as a dummy for potentially invalid date - 9999-12-31 23:59:59.9999999
  lazy val dummyTimestamp = new Timestamp(8099, 11, 31, 23, 59, 59, 99999999)
  /// log identifier of the current execution
  lazy val logKey = sdfConcat.format(Calendar.getInstance.getTime)
    .concat(RandomUtils.nextLong(1000L, 9000L).toString) 

  lazy val spark = SparkSession
      .builder()
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("parquet.column.index.access", "true")             // ensure hive schema is enforced instead of parquet files
      .config("spark.sql.hive.convertMetastoreParquet", "false") // ensure hive schema is enforced instead of parquet files
      .enableHiveSupport()
      .getOrCreate()
  lazy val sparkContext = spark.sparkContext
  lazy val hadoopConfiguration = sparkContext.hadoopConfiguration

  lazy val yyEnd = rn.substring(0, 4).toInt
  lazy val yyPrv = yyEnd - 1
  lazy val mmEnd = rn.substring(4, 6).toInt
  lazy val tsEnd = new Timestamp(yyEnd - 1900, mmEnd, 0, 23, 59, 59, 999999999)
  lazy val tsStrt = new Timestamp(yyEnd - 1900, mmEnd - numOfMnth, 1, 0, 0, 0, 0)
  lazy val ymEnd = sdfYrMnth.format(tsEnd)
  lazy val ymStrt = sdfYmMnth.format(tsStrt)

  lazy val ymYtd = 1 to mmEnd map(x => f"$yyEnd$x%02d")
  lazy val ymPYr = 1 to 12 map(x => f"$yyPrv$x%02d")
  lazy val ymYtdQtr = (0 to 3 map(x => Seq(x*3+1, x*3+2, x*3+3))).map(_.map(x => f"$yyEnd$x%02d"))
  lazy val ymPYQtr = (0 to 3 map(x => Seq(x*3+1, x*3+2, x*3+3))).map(_.map(x => f"$yyPrv$x%02d"))
  lazy val ymMap:Map[String, Seq[String]] = Map(
    "ytd" -> ymYtd,
    "q1" -> ymYtdQtr(0),
    "q2" -> ymYtdQtr(1),
    "q3" -> ymYtdQtr(2),
    "q4" -> ymYtdQtr(3),
    "pyr" -> ymPYr,
    "pq1" -> ymPYQtr(0), 
    "pq2" -> ymPYQtr(1), 
    "pq3" -> ymPYQtr(2), 
    "pq4" -> ymPYQtr(3)) 
}
