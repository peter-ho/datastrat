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
import com.datastrat.util.Session._
import com.datastrat.util.SessionInstance
import com.datastrat.util.SqlExt._
import com.datastrat.util.Execution._

/**  A generic class for boot-strapping the overall flow of Extract Transform Load implementations
 */
abstract class ETLStrategy(env: String, conf: Map[String, String], sess: SparkSession, subj_area_nm:String, tgtTbl: (String, String), src_tbl_nms:Array[(String, String)]=Array(), colPart:Seq[String]=Seq()) extends ETLTrait {

  lazy val locations: Map[String, String] = Map(
      "inbound" -> (conf.get("pth.inbound").get),
      "stage" -> (conf.get("pth.stage").get),
      "work" -> (conf.get("pth.work").get),
      "reference" -> (conf.get("pth.reference").get),
      "warehouse" -> (conf.get("pth.warehouse").get),
      "outbound" -> (conf.get("pth.outbound").get),
      "archive" -> (conf.get("pth.archive").get))
  lazy val dbNms: Map[String, String] = Map(
      "inbound" -> (conf.get("db.inbound").get),
      "stage" -> (conf.get("db.stage").get),
      "work" -> (conf.get("db.work").get),
      "reference" -> (conf.get("db.reference").get),
      "warehouse" -> (conf.get("db.warehouse").get),
      "outbound" -> (conf.get("db.outbound").get),
      "archive" -> (conf.get("db.archive").get))
  def tn(tblNm:(String, String)):String = s"${dbNms(tblNm._1)}${tblNm._2}"

  /** Override base trait importData function for the process to start executing the following steps:
   *  setup parameters, execute transformation, write resulting [[org.apache.spark.sql.DataFrame]] to disk, log result
   */
  override def extract(args: Array[String]): AuditLog = {
    println(s" ... extract starting with args: ${args.mkString(",")}")
    val start = Calendar.getInstance.getTime
    val tsStart = new java.sql.Timestamp(start.getTime)
    val loadNbr = conf.getOrElse("load.nbr", "")
    if (loadNbr.length > 0) {
      Current = new SessionInstance(new java.sql.Timestamp(sdfConcat.parse(loadNbr).getTime))
    }

    val llk = logKey
    val usrNm = System.getProperty("user.name")
    val tgtTblNm = dbNms(tgtTbl._1) + tgtTbl._2
    var al:AuditLog = null
    try {
      val result = extractInternal(args)
      val alEmpty = AuditLog(llk, Current.loadNbr, subj_area_nm, Array(), tgtTblNm, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), 0, 0, "no data returned", result.load_type, JobStatus.Failure, usrNm)
      al = logExecution(if (result.data.isEmpty) alEmpty else {
        result.data.get.persist(StorageLevel.MEMORY_AND_DISK_SER)
        result.data.get.printSchema
        if (args.length > 1 && args(1) == "report") result.data.get.report()
        if (result.data.get.count == 0) alEmpty
        else writeToTable(result, start, tgtTbl._1, tgtTbl._2, Current.loadNbr, true, s"load_nbr=${Current.loadNbr}/load_log_key=$llk")
      })
    } catch {
      	case e:Throwable => {
          e.printStackTrace
          val sb = new StringBuilder
          fillExcpMsg(sb, e)
          al = logExecution(AuditLog(llk, Current.loadNbr, subj_area_nm, Array(), tgtTblNm, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), 0, 0, sb.toString, "", JobStatus.Failure, usrNm))
        }
    }
    al
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
  def extractInternal(args: Array[String]): ExtractResult = {
    import sess.implicits._
    val res = extractPrd(null, extrctSnglPrd)
    ExtractResult(null, Some(res), src_tbl_nms.map(x => s"${dbNms(x._1)}${x._2}"), "CoreToCore")
  }


  /** execute a given extract for a predefined period
    *
    * @example 1
    *          {{{extractPrd(df, "ytd", "YTD", "C", f)}}}
    * @param dfMmd data frame with monthly detail
    * @param ymKey key to refresh header ymMap to get a range of ym as filter
    * @param prdTyp type of period e.g. YTD, QTR, etc..
    * @param currOrPrevPrd C for current, P for previoud etc..
    * @param exe function to execute to provide a dataframe to be unioned together with a given set of year month ids
    * @return a data frame with data frame joined together with a specific timeframe
    */
  def extractPrd(dfMmd:DataFrame, ymKey:String, prdTyp:String, currOrPrevPrd:String, exe:(DataFrame, Seq[String])=>DataFrame):DataFrame = {
    val ymFltr = Current.ymMap(ymKey)
    println(s"...start loading prd for $ymFltr")
    exe(dfMmd, ymFltr).withColumn("prd_type", lit(prdTyp))
      .withColumn("curnt_or_prev_prd", lit(currOrPrevPrd))
  }

  /** execute a given extract for a set of predefined period
    *
    * @example 1
    *          {{{exePrd(df, f)}}}
    * @param dfMmd data frame with monthly detail
    * @param exe function to execute to provide a dataframe to be unioned together with a given set of year month ids
    * @return a data frame with data frame joined together with multiple timeframe
    */
  def extractPrd(dfMmd:DataFrame, exe:(DataFrame,Seq[String])=>DataFrame):DataFrame = {
    var df = extractPrd(dfMmd, "ytd", "YTD", "C", exe).withColumn("prd_nm", lit("YTD"))
    1 to 4 foreach(y => {
      println(s"...start loading quarter $y")
      val d = extractPrd(dfMmd, s"q$y", "QTR", "C", exe).withColumn("prd_nm",
        lit(s"Q$y ${Current.yyEnd}"))
      //d.show
      df = df.union(d)
    })
    4 to 4 foreach(y => {
      println(s"...start loading previous year quarter $y")
      val d = extractPrd(dfMmd, s"pq$y", "QTR", "P", exe).withColumn("prd_nm",
        lit(s"Q$y ${Current.yyPrv}"))
      df = df.union(d)
    })
    df.union(extractPrd(dfMmd, "pyr", "YTD", "P", exe).withColumn("prd_nm", lit("YTD")))
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
    * @param loadNbr number as an identifier of the current load 
    * @param unpersistDataAfter true if data frame should be unpersisted, false otherwise
    *          {{{writeToTable(df, start, "wk", "table1")}}}
    * @param partitionHdfsSubpath part of the hdfs path that identifies the partition this data to be written to
    */
  def writeToTable(result: ExtractResult, start: java.util.Date, dbKey: String, tblNm: String, loadNbr:String, unpersistDataAfter: Boolean, partitionHdfsSubpath: String = null): AuditLog  = {
    writeToTable(result, start, dbKey, tblNm, loadNbr, getTableHdfsPath(dbKey, tblNm, partitionHdfsSubpath), unpersistDataAfter)
  }

  /** writes a given [[org.apache.spark.sql.DataFrame]] instance to a specified Hive table overwriting existing data in a specific hdfs path potentially for partitioned data, execution is also logged with end time as current date time after log is executed. 
    *
    * @example 1
    *          {{{writeToTable(ExtractResult("Extract success or failed due to ... ", Some(df), Array("tbl1", "tbl2")), start, "awh", "table1", "/ts/data/warehouse/tbl1", true)}}}
    * @param result [[com.datastrat.hgcr.etl.ExtractResult]] instance to be written as content of a Hive table with current data be overwritten inicluding comment, data and source information
    * @param start [[java.util.Date]] instance specifying the date time the job starts
    * @param dbKey key of the database for this [[org.apache.spark.sql.DataFrame]] to be written to, e.g. "wk"
    * @param tblNm name of the Hive table to be written to 
    * @param loadNbr number as an identifier of the current load 
    * @param hdfsPath path for the data to be written to
    * @param unpersistDataAfter true if data frame should be unpersisted, false otherwise
    */
  def writeToTable(result: ExtractResult, start: java.util.Date, dbKey: String, tblNm: String, loadNbr: String, hdfsPath: String, unpersistDataAfter: Boolean): AuditLog = {
    val sb = new StringBuilder(s"yarn application id: ${sess.sparkContext.applicationId}\nresult comment: ${result.comment}\n")
    val end = Calendar.getInstance.getTime
    val tgtTblNm = dbNms(dbKey) + tblNm
    val dfOrig = sess.table(tgtTblNm)
    val tgtOrigCnt = dfOrig.count
    var rowCount:Long = 0
    val ds = result.data.get
    val cols = sess.catalog.listColumns(tgtTblNm).select("name").collect
      .map(x=>x.getString(0)).filter(x => x != "load_log_key" && x != "load_nbr")
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
    postWriteSetup(dbKey, tblNm, tgtTblNm, loadNbr, hdfsPath, ds, cols)
    if (unpersistDataAfter) ds.unpersist
    AuditLog(logKey, Current.loadNbr, subj_area_nm, result.src_tbl_nms, tgtTblNm, new java.sql.Timestamp(start.getTime), new java.sql.Timestamp(end.getTime), rowCount, tgtOrigCnt, sb.toString, result.load_type, if (archResult._1 > 1) JobStatus.Failure else JobStatus.Success, System.getProperty("user.name"))
  }

  def writeValidation(result: ExtractResult, archResult: (Long, String), tgtTblNm: String, loadNbr: String): String = {
    "test"
  }

  def postWriteSetup(dbKey:String, tblNm:String, tgtTblNm:String, loadNbr:String, hdfsPath:String, ds:DataFrame, cols:Array[String]): String = { 
    val fs = FileSystem.get(hadoopConfiguration)
    println(s" === PostWriteSetup for $tgtTblNm $loadNbr $hdfsPath $cols")
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
      .withColumn("load_nbr", 'partition(0)).withColumn("load_log_key", 'partition(1))
      .select("load_nbr", "load_log_key").cache
    val loads = parts.orderBy("load_log_key").collect().dropRight(1)
    //val runIds = dfOrig.select("load_nbr", "load_log_key").distinct.collect
    val fs = FileSystem.get(hadoopConfiguration)
    if (dbKey == "stage") {
      println(" === no archive for stage, so just delete current data == ")
      loads.foreach(x => {
        val spath = s"${locations(dbKey)}$tblNm/load_nbr=${x.getString(0)}/load_log_key=${x.getString(1)}"
        val path = new Path(spath)
        sbMsg.append(retry(() => fs.delete(path, true), s"delete of $spath failed ", 3))
        sess.sql(s"alter table ${dbNms(dbKey)}$tblNm drop partition(load_nbr='${x.getString(0)}', load_log_key='${x.getString(1)}')")
      })
    } else {
      println(" === create directory for each rfresh number == ")
      parts.select("load_nbr").distinct.collect().map(_.getString(0)).foreach(x => {
        fs.mkdirs(new Path(s"${locations("archive")}$tblNm/load_nbr=$x"))
      })
      println(" === move/rename current load_nbr/run ")
      loads.foreach(x => {
        val spath = s"$tblNm/load_nbr=${x.getString(0)}/load_log_key=${x.getString(1)}"
        val srcPath = new Path(s"${locations(dbKey)}$spath")
        val destPath = new Path(s"${locations("archive")}$spath")
        sbMsg.append(retry(() => fs.rename(srcPath, destPath), s"move of $spath failed ", 3))
        sess.sql(s"alter table ${dbNms(dbKey)}$tblNm drop partition(load_nbr='${x.getString(0)}', load_log_key='${x.getString(1)}')")
      })
      sess.sql(s"msck repair table ${dbNms("archive")}$tblNm")
      val rnToDel = sess.table(dbNms("archive") + tblNm).select("load_nbr").distinct.orderBy("load_nbr").collect().map(_.getString(0)).dropRight(4)
      rnToDel.foreach(x => {
        val spath = s"${locations("archive")}$tblNm/load_nbr=$x"
        val archPath = new Path(spath)
        sbMsg.append(retry(() => fs.delete(archPath, true), s"deletion of extra archive file failed ", 3))
        sess.sql(s"alter table ${dbNms("archive")}$tblNm drop partition(load_nbr='$x')")
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
  def main(args: Array[String]) {
    if (args.length < 3) throw new MissingArgumentException("class name, environment (dev, tst, uat, prd) are required as a parameter, organization, and area are required.")
    else {
      val cnstrs  = Class.forName(args(0)).getConstructors()
      if (cnstrs.length == 0) throw new Exception(s"Constructor not found in class ${args(0)}")
      val cnstr = cnstrs(0)
      val env = args(1)
      val org = args(2)
      val ara = args(3)
      val conf =  ConfLoader(env, org, ara)
      conf.put("load.nbr", if (args.length == 4) "" else args(4))
      println(s" ... execution starts [${args(0)}] ${conf.getOrElse("load.nbr", "")}")
      val stra = cnstr.newInstance(env, org, ara, conf.toMap, spark).asInstanceOf[ETLTrait]
      val al = stra.extract(args.slice(4, args.length))
      println(al)
      if (JobStatus.Success.toString != al.status) {
        sys.exit(1)
      }
    }
  }
}
