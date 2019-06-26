/*
 * Copyright (c) 2019, All rights reserved.
 * @author Peter Ho
 *
 */
package com.datastrat.etl

import sys.process._
import java.util.{Date, Calendar, Properties}
import java.sql.{ Connection, DriverManager }  
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
import com.datastrat.util.ConfLoader

class RfrshHdr(rn: String, numOfMnth: Int = 38) {
  lazy val frmtRn = new java.text.SimpleDateFormat("yyyyMM")
  lazy val yyEnd = rn.substring(0, 4).toInt
  lazy val yyPrv = yyEnd - 1
  lazy val mmEnd = rn.substring(4, 6).toInt
  lazy val tsEnd = new java.sql.Timestamp(yyEnd - 1900, mmEnd, 0, 23, 59, 59, 999999999)
  lazy val tsStrt = new java.sql.Timestamp(yyEnd - 1900, mmEnd - numOfMnth, 1, 0, 0, 0, 0)
  lazy val ymEnd = frmtRn.format(tsEnd)
  lazy val ymStrt = frmtRn.format(tsStrt)

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

case class ExtractResult (comment: String, data: Option[DataFrame], src_tbl_nms:  Array[String], load_type: String) {
  def this(result:ExtractResult, data:Option[DataFrame]) = this(result.comment, data, result.src_tbl_nms, result.load_type)
}


/**  A generic class for boot-strapping the overall flow of Extract Transform Load implementations
 */
abstract class ETLStrategy(env: String, conf: Map[String, String], sess: SparkSession, subj_area_nm:String, tgtTbl: (String, String), src_tbl_nms:Array[(String, String)]=Array(), colPart:Seq[String]=Seq()) extends ETLTrait {
  lazy val sdFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  lazy val sdFormat2 = new SimpleDateFormat("yyyyMM")
  lazy val dummyTimestamp = new java.sql.Timestamp(6988, 11, 31, 0, 0, 0, 0) //8888-12-31 00:00:00

  lazy val locations: Map[String, String] = Map(
      "source" -> (conf.get("fs-source").get),
      "stage" -> (conf.get("fs-stage").get),
      "core" -> (conf.get("fs-core").get),
      "work" -> (conf.get("fs-work").get),
      "archive" -> (conf.get("fs-archive").get))
  lazy val dbNms: Map[String, String] = Map(
      "source" -> (conf.get("db-source").get),
      "source_ref" -> (conf.get("db-source-ref").get),
      "stage" -> (conf.get("db-stage").get),
      "core" -> (conf.get("db-core").get),
      "work" -> (conf.get("db-work").get),
      "archive" -> (conf.get("db-archive").get))
  def tn(tblNm:(String, String)):String = s"${dbNms(tblNm._1)}${tblNm._2}"

  /** Override base trait importData function for the process to start executing the following steps:
   *  setup parameters, execute transformation, write resulting [[org.apache.spark.sql.DataFrame]] to disk, log result
   */
  override def extract(args: Array[String]): AuditLog = {
    println(s" ... extract starting with args: ${args.mkString(",")}")
    val start = Calendar.getInstance.getTime
    val tsStart = new java.sql.Timestamp(start.getTime)
    val rfrshNbr = conf.getOrElse("rfrsh-nbr", "")
    val llk = getLoadLogKey
    val usrNm = System.getProperty("user.name")
    val tgtTblNm = dbNms(tgtTbl._1) + tgtTbl._2
    var al:AuditLog = null
    try {
      val result = extractInternal(new RfrshHdr(rfrshNbr), args)
      val alEmpty = AuditLog(llk, rfrshNbr, subj_area_nm, Array(), tgtTblNm, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), 0, 0, "no data returned", result.load_type, JobStatus.Failure, usrNm)
      al = logExecution(if (result.data.isEmpty) alEmpty else {
        result.data.get.persist(StorageLevel.MEMORY_AND_DISK_SER)
        result.data.get.printSchema
        if (args.length > 1 && args(1) == "report") report(result.data.get)
        if (result.data.get.count == 0) alEmpty
        else writeToTable(result, start, tgtTbl._1, tgtTbl._2, rfrshNbr, true, s"rfrsh_nbr=$rfrshNbr/load_log_key=$llk")
        //writeToTable2(result, start, tgtTbl._1, tgtTbl._2, rfrshNbr, llk, true)
      })
    } catch {
      	case e:Throwable => {
          e.printStackTrace
          val sb = new StringBuilder
          fillExcpMsg(sb, e)
          al = logExecution(AuditLog(llk, rfrshNbr, subj_area_nm, Array(), tgtTblNm, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), 0, 0, sb.toString, "", JobStatus.Failure, usrNm))
        }
    }
    al
  }

  def fillExcpMsg(sb:StringBuilder, e:Throwable):Unit = {
    sb.append(e.toString).append(e.getStackTraceString).append("\n")
    if (e.getCause != null) fillExcpMsg(sb, e.getCause)
  }

  /** based on the base [[org.apache.spark.sql.DataFrame]], the appnd [[org.apache.spark.sql.DataFrame*]] is union to the end, with any missing column(s) added as null
    *
    * @return an instance of [[org.apache.spark.sql.DataFrame]] with base and appnd union together
    */
  def union(base:DataFrame, appnd:DataFrame*):DataFrame = {
    base.union(appnd.map(x => x.select((base.columns.intersect(x.columns).map(x(_)) ++ base.columns.diff(x.columns).map(lit(null).as(_))):_*).select(base.columns.head, base.columns.tail:_*)).reduce((x,y) => x.union(y)))
  }

  def report(df: DataFrame) = {
    val cols = df.columns
    cols.foreach(x => {
      df.groupBy(x).agg(count(x).as("cnt")).orderBy(desc("cnt")).show(50, false)
    })
  }

  def extrctSnglPrd(dfMd:DataFrame, ymFltr:Seq[String]): DataFrame = {
    null
  }

  /** based on the given configuration, read from data source, load and transform it to a [[org.apache.spark.sql.DataFrame]], to be marked as abstract, transform is responsible for error logging and if an error occured, None is returned
    *
    * @example 1
    *          {{{extract(sess, dbNms)}}}
    * @return an instance of [[com.datastrat.hgcr.etl.ExtractResult]]
    */
  def extractInternal(hdr: RfrshHdr, args: Array[String]): ExtractResult = {
    import sess.implicits._
    val res = extractPrd(null, hdr, extrctSnglPrd)
    ExtractResult(null, Some(res), src_tbl_nms.map(x => s"${dbNms(x._1)}${x._2}"), "CoreToCore")
  }

  /** Get the [[org.apache.spark.sql.expressions.WindowSpec]] instance for windowing functions based on partition columns and order by columns
    *
    * @return an instance of [[org.apache.spark.sql.expressions.WindowSpec]] supporting the provided partitionBy and orderBy columns
    */
  def getWindowExpr(partitionCols: Seq[Column], orderStmnt: Seq[Column]): WindowSpec = {
    Window.partitionBy(cols=partitionCols:_*).orderBy(cols=orderStmnt:_*)
  }

  /** Get the current instance of Hadoop configuration
    *
    * @return an existing instance of [[org.apache.hadoop.conf.Configuration]]
    */
  def getHadoopConfiguration: Configuration = {
    sess.sparkContext.hadoopConfiguration
  }
  
  /** Get the generated load log key of the current run
    *
    * @return the generated load log key of the current run
    */
  def getLoadLogKey: String = {
    ETLStrategy.LoadLogKey
  }

  def debug(name:String, df:DataFrame):DataFrame = {
    df.cache
    println(s"$name count: ${df.count}")
    df.show(150, false)
    //df.write.mode(SaveMode.Overwrite).parquet(s"${locations("wk")}/$name")
    df
  }
  

  /** execute a given extract for a predefined period
    *
    * @example 1
    *          {{{extractPrd(df, hdr, "ytd", "YTD", "C", f)}}}
    * @param dfMmd data frame with monthly detail
    * @param hdr refresh header with date specific detail
    * @param ymKey key to refresh header ymMap to get a range of ym as filter
    * @param prdTyp type of period e.g. YTD, QTR, etc..
    * @param currOrPrevPrd C for current, P for previoud etc..
    * @param exe function to execute to provide a dataframe to be unioned together with a given set of year month ids
    * @return a data frame with data frame joined together with a specific timeframe
    */
  def extractPrd(dfMmd:DataFrame, hdr:RfrshHdr, ymKey:String, prdTyp:String, currOrPrevPrd:String, exe:(DataFrame, Seq[String])=>DataFrame):DataFrame = {
    val ymFltr = hdr.ymMap(ymKey)
    println(s"...start loading prd for $ymFltr")
    exe(dfMmd, ymFltr).withColumn("prd_type", lit(prdTyp))
      .withColumn("curnt_or_prev_prd", lit(currOrPrevPrd))
  }

  /** execute a given extract for a set of predefined period
    *
    * @example 1
    *          {{{exePrd(df, hdr, f)}}}
    * @param dfMmd data frame with monthly detail
    * @param hdr refresh header with date specific detail
    * @param exe function to execute to provide a dataframe to be unioned together with a given set of year month ids
    * @return a data frame with data frame joined together with multiple timeframe
    */
  def extractPrd(dfMmd:DataFrame, hdr:RfrshHdr, exe:(DataFrame,Seq[String])=>DataFrame):DataFrame = {
    var df = extractPrd(dfMmd, hdr, "ytd", "YTD", "C", exe).withColumn("prd_nm", lit("YTD"))
    1 to 4 foreach(y => {
      println(s"...start loading quarter $y")
      val d = extractPrd(dfMmd, hdr, s"q$y", "QTR", "C", exe).withColumn("prd_nm",
        lit(s"Q$y ${hdr.yyEnd}"))
      //d.show
      df = df.union(d)
    })
    4 to 4 foreach(y => {
      println(s"...start loading previous year quarter $y")
      val d = extractPrd(dfMmd, hdr, s"pq$y", "QTR", "P", exe).withColumn("prd_nm",
        lit(s"Q$y ${hdr.yyPrv}"))
      df = df.union(d)
    })
    df.union(extractPrd(dfMmd, hdr, "pyr", "YTD", "P", exe).withColumn("prd_nm", lit("YTD")))
  }

  /** get the hdfs path of a given Hive table
    *
    * @example 1
    *          {{{getTableHdfsPath("wk", "wk_prov_npi3")}}}
    * @param dbKey key of the database
    * @param tblName name of the Hive table
    * @param partitionHdfsSubpath part of the hdfs path that identifies the partition this data to be written to
    * @return hdfs path of the specified Hive table
    */
  def getTableHdfsPath(dbKey: String, tblName: String, partitionHdfsSubpath: String = null): String = {
    if (partitionHdfsSubpath == null) s"""${locations(dbKey)}/$tblName""" else s"""${locations(dbKey)}/$tblName/$partitionHdfsSubpath"""
  }


  /** writes a given [[org.apache.spark.sql.DataFrame]] instance to a specified Hive table overwriting existing data, execution is also logged with end time as current date time after log is executed. 
    *
    *
    * @example 1
    *          {{{writeToTable(ExtractResult("Extract success or failed due to ... ", Some(df), Array("tbl1", "tbl2")), start, "awh", "table1", true)}}}
    * @param result [[com.datastrat.hgcr.etl.ExtractResult]] instance to be written as content of a Hive table with current data be overwritten inicluding comment, data and source information
    * @param start [[java.util.Date]] instance specifying the date time the job starts
    * @param dbKey key of the database for this [[org.apache.spark.sql.DataFrame]] to be written to, e.g. "wk"
    * @param tblNm name of the Hive table to be written to 
    * @param unpersistDataAfter true if data frame should be unpersisted, false otherwise
    *          {{{writeToTable(df, start, "wk", "table1")}}}
    * @param partitionHdfsSubpath part of the hdfs path that identifies the partition this data to be written to
    */
  def writeToTable(result: ExtractResult, start: java.util.Date, dbKey: String, tblNm: String, rfrshNbr: String, unpersistDataAfter: Boolean, partitionHdfsSubpath: String = null): AuditLog  = {
    writeToTable(result, start, dbKey, tblNm, rfrshNbr, getTableHdfsPath(dbKey, tblNm, partitionHdfsSubpath), unpersistDataAfter)
  }

  /** writes a given [[org.apache.spark.sql.DataFrame]] instance to a specified Hive table overwriting existing data in a specific hdfs path potentially for partitioned data, execution is also logged with end time as current date time after log is executed. 
    *
    * @example 1
    *          {{{writeToTable2(ExtractResult("Extract success or failed due to ... ", Some(df), Array("tbl1", "tbl2")), start, "awh", "table1", "/ts/data/warehouse/tbl1", true)}}}
    * @param result [[com.datastrat.hgcr.etl.ExtractResult]] instance to be written as content of a Hive table with current data be overwritten inicluding comment, data and source information
    * @param start [[java.util.Date]] instance specifying the date time the job starts
    * @param dbKey key of the database for this [[org.apache.spark.sql.DataFrame]] to be written to, e.g. "wk"
    * @param tblNm name of the Hive table to be written to 
    * @param hdfsPath path for the data to be written to
    * @param unpersistDataAfter true if data frame should be unpersisted, false otherwise
    */
  def writeToTable2(result: ExtractResult, start: java.util.Date, dbKey: String, tblNm: String, rfrshNbr: String, llk: String, unpersistDataAfter: Boolean): AuditLog = {
    val sb = new StringBuilder(s"yarn application id: ${sess.sparkContext.applicationId}\nresult comment: ${result.comment}\n")
    val end = Calendar.getInstance.getTime
    val tgtTblNm = dbNms(dbKey) + tblNm
    val dfOrig = sess.table(tgtTblNm)
    val tgtOrigCnt = dfOrig.count
    var rowCount:Long = 0
    if (!result.data.isEmpty) {
      val ds = result.data.get
      rowCount = ds.count
      ds.show
      val col = ds.columns.filter(!colPart.contains(_)) ++ Seq("rfrsh_nbr", "load_log_key") ++ colPart
      println("columns to select:: ")
      col.foreach(println)
      ds.select("mdcl_expsr_nbr").distinct.show(50)
      ds.withColumn("rfrsh_nbr", lit(rfrshNbr)).withColumn("load_log_key", lit(llk))
        .select(col.head, col.tail:_*)
        .write.format("parquet").mode(SaveMode.Overwrite).insertInto(tgtTblNm)
      //ds.write.mode(SaveMode.Overwrite).parquet(hdfsPath)
      sess.sql(s"msck repair table $tgtTblNm")
      if (unpersistDataAfter) ds.unpersist
    }
    val archResult = archive(dbKey, tblNm)
    sb.append(s"number of partitions after archive: ${archResult._1}\n")
    sb.append(archResult._2)
    
    AuditLog(getLoadLogKey, rfrshNbr, subj_area_nm, result.src_tbl_nms, tgtTblNm, new java.sql.Timestamp(start.getTime), new java.sql.Timestamp(end.getTime), rowCount, tgtOrigCnt, sb.toString, result.load_type, if (archResult._1 > 1) JobStatus.Failure else JobStatus.Success, System.getProperty("user.name"))
  }

  /** writes a given [[org.apache.spark.sql.DataFrame]] instance to a specified Hive table overwriting existing data in a specific hdfs path potentially for partitioned data, execution is also logged with end time as current date time after log is executed. 
    *
    * @example 1
    *          {{{writeToTable(ExtractResult("Extract success or failed due to ... ", Some(df), Array("tbl1", "tbl2")), start, "awh", "table1", "/ts/data/warehouse/tbl1", true)}}}
    * @param result [[com.datastrat.hgcr.etl.ExtractResult]] instance to be written as content of a Hive table with current data be overwritten inicluding comment, data and source information
    * @param start [[java.util.Date]] instance specifying the date time the job starts
    * @param dbKey key of the database for this [[org.apache.spark.sql.DataFrame]] to be written to, e.g. "wk"
    * @param tblNm name of the Hive table to be written to 
    * @param hdfsPath path for the data to be written to
    * @param unpersistDataAfter true if data frame should be unpersisted, false otherwise
    */
  def writeToTable(result: ExtractResult, start: java.util.Date, dbKey: String, tblNm: String, rfrshNbr: String, hdfsPath: String, unpersistDataAfter: Boolean): AuditLog = {
    val sb = new StringBuilder(s"yarn application id: ${sess.sparkContext.applicationId}\nresult comment: ${result.comment}\n")
    val end = Calendar.getInstance.getTime
    val tgtTblNm = dbNms(dbKey) + tblNm
    val dfOrig = sess.table(tgtTblNm)
    val tgtOrigCnt = dfOrig.count
    var rowCount:Long = 0
    val ds = result.data.get
    val cols = sess.catalog.listColumns(tgtTblNm).select("name").collect
      .map(x=>x.getString(0)).filter(x => x != "load_log_key" && x != "rfrsh_nbr")
    ds.show
    val d = ds.select(cols.head, cols.tail:_*).distinct.cache
    ds.unpersist
    rowCount = d.count
    d.show
    d.write.mode(SaveMode.Overwrite).parquet(hdfsPath)
    sess.sql(s"msck repair table $tgtTblNm")
    val archResult = archive(dbKey, tblNm)
    sb.append(s"number of partitions after archive: ${archResult._1}\n")
    sb.append(archResult._2)
    postWriteSetup(dbKey, tblNm, tgtTblNm, rfrshNbr, hdfsPath, ds, cols)
    if (unpersistDataAfter) ds.unpersist
    AuditLog(getLoadLogKey, rfrshNbr, subj_area_nm, result.src_tbl_nms, tgtTblNm, new java.sql.Timestamp(start.getTime), new java.sql.Timestamp(end.getTime), rowCount, tgtOrigCnt, sb.toString, result.load_type, if (archResult._1 > 1) JobStatus.Failure else JobStatus.Success, System.getProperty("user.name"))
  }

  def writeValidation(result: ExtractResult, archResult: (Long, String), tgtTblNm: String, rfrshNbr: String): String = {
    "test"
  }

  def postWriteSetup(dbKey:String, tblNm:String, tgtTblNm:String, rfrshNbr:String, hdfsPath:String, ds:DataFrame, cols:Array[String]): String = { 
    val fs = FileSystem.get(getHadoopConfiguration)
    println(s" === PostWriteSetup for $tgtTblNm $rfrshNbr $hdfsPath $cols")
    val bsPth = getTableHdfsPath(dbKey, tblNm)
    println(s"   attempt clean up of $bsPth")
    fs.listStatus(new Path(bsPth)).filter(_.isDir).filter(x => fs.listStatus(x.getPath).size == 0)
      .foreach(x => {
        print(s"    Trying to delete: ${x.getPath} : ")
        println(if (fs.delete(x.getPath, false)) "Success" else "Fail")
    })
    
    sess.sql(s"analyze table $tgtTblNm compute statistics for columns ${cols.mkString(",")}")
    println(s"statistics computed for table: $tgtTblNm")
    //TODO: potential integration with impala within scala code
    //val cmd = "impala-shell -i sl01plvbic001.wellpoint.com -d default -k --ssl --ca_cert=/opt/cloudera/security/CAChain.pem".split(" ").toList
    //(cmd ::: List("-q", s"invalidate metadata $tgtTblNm")).!
    //(cmd ::: List("-q", s"compute stats $tgtTblNm")).!
    "Pass"
  }

  def retry(execution:()=>Boolean, msg: String, retry_count:Integer): String = {
    val sb = new StringBuilder
    var tryCount = 1
    while (tryCount < retry_count && !execution()) {
      sb.append(s"$msg at try $tryCount\n")
      tryCount += 1
    }
    sb.toString
  }

  def archive(dbKey: String, tblNm: String): (Long, String) = {
    import sess.implicits._
    val sbMsg = new StringBuilder
    /// refresh number and load log key are assumed to be the first two partitions
    val parts = sess.sql(s"show partitions ${dbNms(dbKey)}$tblNm")
      .withColumn("partition", split(regexp_replace('partition, "[^\\/]+?=", ""), "/"))
      .withColumn("rfrsh_nbr", 'partition(0)).withColumn("load_log_key", 'partition(1))
      .select("rfrsh_nbr", "load_log_key").cache
    val loads = parts.orderBy("load_log_key").collect().dropRight(1)
    //val runIds = dfOrig.select("rfrsh_nbr", "load_log_key").distinct.collect
    val fs = FileSystem.get(getHadoopConfiguration)
    if (dbKey == "stage") {
      println(" === no archive for stage, so just delete current data == ")
      loads.foreach(x => {
        val spath = s"${locations(dbKey)}$tblNm/rfrsh_nbr=${x.getString(0)}/load_log_key=${x.getString(1)}"
        val path = new Path(spath)
        sbMsg.append(retry(() => fs.delete(path, true), s"delete of $spath failed ", 3))
        //while (!fs.delete(new Path(path), true)) {
        //  sb.append(s"delete of ${locations(dbKey)}$tblNm/rfrsh_nbr=$x failed at try #$tryCount")
        //}
        sess.sql(s"alter table ${dbNms(dbKey)}$tblNm drop partition(rfrsh_nbr='${x.getString(0)}', load_log_key='${x.getString(1)}')")
      })
    } else {
      println(" === create directory for each rfresh number == ")
      parts.select("rfrsh_nbr").distinct.collect().map(_.getString(0)).foreach(x => {
        fs.mkdirs(new Path(s"${locations("archive")}$tblNm/rfrsh_nbr=$x"))
      })
      println(" === move/rename current rfrsh_nbr/run ")
      loads.foreach(x => {
        val spath = s"$tblNm/rfrsh_nbr=${x.getString(0)}/load_log_key=${x.getString(1)}"
        val srcPath = new Path(s"${locations(dbKey)}$spath")
        val destPath = new Path(s"${locations("archive")}$spath")
        sbMsg.append(retry(() => fs.rename(srcPath, destPath), s"move of $spath failed ", 3))
        //if (!fs.rename(new Path(s"${locations(dbKey)}$path"), new Path(s"${locations("archive")}$path"))) {
        //}
        sess.sql(s"alter table ${dbNms(dbKey)}$tblNm drop partition(rfrsh_nbr='${x.getString(0)}', load_log_key='${x.getString(1)}')")
      })
      sess.sql(s"msck repair table ${dbNms("archive")}$tblNm")
      val rnToDel = sess.table(dbNms("archive") + tblNm).select("rfrsh_nbr").distinct.orderBy("rfrsh_nbr").collect().map(_.getString(0)).dropRight(4)
      rnToDel.foreach(x => {
        val spath = s"${locations("archive")}$tblNm/rfrsh_nbr=$x"
        val archPath = new Path(spath)
        sbMsg.append(retry(() => fs.delete(archPath, true), s"deletion of extra archive file failed ", 3))
        sess.sql(s"alter table ${dbNms("archive")}$tblNm drop partition(rfrsh_nbr='$x')")
      })
    }
    val partCnt = sess.sql(s"show partitions ${dbNms(dbKey)}$tblNm").count
    println(s" === archive completed with partition count: $partCnt")
    (partCnt, sbMsg.toString)
  }

  /** log execution to audit_log table with the given information about the load    *
    * @example 1
    *          {{{logExecution(AuditLog("201810161421323232", Array("source1, source2"), "wh.target1", start, end, 232100313, 23213111, "LoadToCore")}}}
    * @param audit [[com.datastrat.etl.AuditLog]] AuditLog instance to be inserted in audit_log table
    */
  def logExecution(audit: AuditLog) : AuditLog = {
    import sess.implicits._
    sess.createDataset(List(audit)).write.mode(SaveMode.Append).parquet(s"${locations("core")}/audit_log")
    audit
  }
}

object ETLStrategy {
  lazy val sdFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  lazy val sdFormatR = new SimpleDateFormat("yyyyMM")
  lazy val LoadLogKey = {
    val currentdateTime = sdFormat.format(Calendar.getInstance.getTime)
    currentdateTime.concat(RandomUtils.nextLong(1000L, 9000L).toString) 
  }
  lazy val defaultRfrshNbr:String = { 
    val c = Calendar.getInstance.getTime
    c.setMonth(c.getMonth -1)
    sdFormatR.format(c)
  }
  lazy val spark = SparkSession
      .builder()
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("parquet.column.index.access", "true")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()
  lazy val sparkContext = spark.sparkContext

  /** replace a given list of columns by its own value within a partition
    *
    * @example 1
    *          {{{replaceWith(d, Seq("val"), Seq(d("id")), Seq(desc("year")))}}}
    * @param df  data frame with the full set of data
    * @param colsToReplace name of columns to be replaced
    * @param prtBy list of columns to be used for partition
    * @param ordBy list of columns to be used for identifying the value to be used for replacement
    * @return a data frame with the same schema and row count but column specified to be replaced with its own based on order and partitioning
    */
  def replaceWith(df:DataFrame, colsToReplace:Seq[String], prtBy:Seq[Column], ordBy:Seq[Column]):DataFrame = {
    var d = df
    val w = Window.partitionBy(prtBy:_*)
    colsToReplace.foreach(x => {
      val rn = s"rn$x"
      val d0 = d.withColumn(rn, row_number().over(w.orderBy(ordBy:_*)))
      d = d0.withColumn(x, when(d0(rn) === 1, d0(x)).otherwise(null))
        .withColumn(x, max(x).over(w)).drop(rn)
    })
    d
  }

  def main(args: Array[String]) {
    if (args.length < 3) throw new MissingArgumentException("class name, environment (dev, tst, uat, prd) are required as a parameter, organization, and area are required.")
    else {
      val cnstrs  = Class.forName(args(0)).getConstructors()
      if (cnstrs.length == 0) throw new Exception(s"Constructor not found in class ${args(0)}")
      val cnstr = cnstrs(0)
      val env = args(1)
      val org = args(2)
      val ara = args(3)
      val confPath = s"/$env/$org/$ara/etl/config/app.properties"
      println(s" ... reading from configuration file in hdfs: $confPath")
      val conf =  ConfLoader(env, org, confPath)
      conf.put("rfrsh-nbr", if (args.length == 4) defaultRfrshNbr else args(4))
      println(s" ... execution starts [${args(0)}] ${conf.getOrElse("rfrsh-nbr", "")}")
      val stra = cnstr.newInstance(env, org, ara, conf.toMap, spark).asInstanceOf[ETLTrait]
      val al = stra.extract(args.slice(4, args.length))
      println(al)
      if (JobStatus.Success.toString != al.status) {
        sys.exit(1)
      }
    }
  }
}
