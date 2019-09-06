/*
 * Copyright (c) 2019, All rights reserved.
 * @author Peter Ho
 *
 */
package com.datastrat.etl

import java.util.Calendar
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocalFileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.io.{File, RandomAccessFile}
import com.datastrat.util.Session._

//import org.apache.spark.sql.types.{StructField, TimestampType}

/**
  * @author Peter Ho
  * @version 1.0
  *
  * Main spark Launcher for file export
  * Parameters: [table name in out bound database] [include header in out if true] [maximum number of file] 
  *   [compression format including none, bzip2, gzip, lz4, snappy] [timestamp format] [line separator default to \r\n] [field separator default to \t]
  */
class Export(env:String, org:String, ara:String, conf:Map[String, String], spark:SparkSession)  extends ETLStrategy(env, conf, spark, ara, null, Array()) with ETLTrait {

  override def execute(args: Array[String]): AuditLog = {
    println(s"Execution starts with arguments ${args.mkString(",")}")
    if (args.length < 2) throw new MissingArgumentException(s"Required parameters missing: ")
    val cmprs = if (args.length > 4) args(4) else "gzip"
    val tsFrmt = if (args.length > 5) args(5) else "yyyy/MM/dd"
    val lnSprtr = if (args.length > 6) args(6) else "\r\n"
    val fldSprtr = if (args.length > 7) args(7) else "\t"
    return execute(args(0), args(1), args(2).toBoolean, args(3).toInt, cmprs, tsFrmt, lnSprtr, fldSprtr)
  }

  /** Export data from hive table and create text delimited files in hdfs  */
  def execute(srcTblNm:String, tblNm:String, prntHdr:Boolean, maxNbrFile:Int, cmprs:String, tsFrmt:String, lnSprtr:String, fldSprtr:String): AuditLog = {
    val start = Calendar.getInstance.getTime
    val tsStart = new java.sql.Timestamp(start.getTime)
    System.setProperty("line.separator",lnSprtr)
    val sb = new StringBuilder
    var js = JobStatus.Failure

    val pth = s"${locations("outbound")}$tblNm/load_id=${Current.loadId}/load_log_key=$logKey"
    //val r = scala.util.Random.nextInt(99999).toString
    //val tempTableName = "tmptsvexport".concat(r)
    val src = tn(srcTblNm)
    val dfT = spark.table(tn(s"outbound.$tblNm"))
    val cols = dfT.columns.filterNot(Array("load_id", "load_log_key").contains(_))
    sb.append(s"\n=== columns identified in target: ${cols.mkString(",")}")
    val d = spark.table(src).select(cols.head, cols.tail:_*)
      .coalesce(maxNbrFile).cache
    sb.append(s"\n=== cache and coalesce source table $src with selected columns")
    val cnt = d.count
    d.write.option("sep", fldSprtr).option("header", prntHdr).option("compression", cmprs)
      .option("timestampFormat", tsFrmt).csv(pth)
    sb.append(s"\n=== wrote table to $pth")
    spark.sql(s"msck repair table ${dbNms("outbound")}$tblNm")
    spark.sql(s"msck repair table ${dbNms("archive")}$tblNm")
    sb.append(s"\n=== msck repair table in hive for new partition")
    archive("outbound", tblNm)
    sb.append(s"\n=== past version archived")
    js = JobStatus.Success

    return logExecution(AuditLog(logKey, Current.loadId, ara, Array(src), tblNm, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), cnt, 0, sb.toString, "E", js, usrNm))
  }
}
