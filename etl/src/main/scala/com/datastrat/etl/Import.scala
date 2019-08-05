package com.datastrat.etl

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.commons.cli.MissingArgumentException
import java.time._
import java.time.format.DateTimeFormatter
import java.util.Calendar
import sys.process._
import com.datastrat.util.ConfLoader
import com.datastrat.util.Session._

/**
 * @author Peter Ho
 * @version 1.0
 *
 * Main Spark Launcher for file import
 * Parameters: [path to file spec file] [number of partition] [input directory location key] [timestamp format]
 */
class Import(env:String, org:String, ara:String, conf:Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, ara, null, Array()) with ETLTrait {

  override def execute(args: Array[String]): AuditLog = {
    if (args.length < 4) throw new MissingArgumentException(s"Required parameters missing: [path to file spec file] [number of partition] [input directory location key] [timestamp format]")
    val numPart = args(1).toInt
    return execute(args(0), numPart, args(2), args(3))
  }

  def execute(filepath: String, numPart: Int, srcLocationKey: String, tsFormat: String): AuditLog = {
    val start = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val tsCurrent = current_timestamp()
    val sb = new StringBuilder
    var js = JobStatus.Failure
    var cnt:Long = -1

    val spec = Spec.loadFileSpec(spark, dbNms("wh"), filepath, true, true, tsFormat)
    val fileType = spec.name
    import spark.implicits._
//1.    New text delimited bot files are loaded in Linux fs on edge node
//2.    process to load from Linux fs to HDFS inbound directory/hive table (table i) overwriting existing files and archiving the incoming files
//    val fs = FileSystem.get(sc.hadoopConfiguration)
//    val srcPath = s"${locationsLocal(srcLocationKey)}${fileType}.csv"
//    if (!java.nio.file.Paths.get(srcPath).toFile().exists())
//      return Some(s"Source file $srcPath not found")
//    fs.copyFromLocalFile(new Path(srcPath), new Path(s"${locations("ib")}${fileType}_inbnd"))
//    s"mkdir -p ${locationsLocal("ba")}".!
//    if (s"mv $srcPath ${locationsLocal("ba")}${fileType}-${LoadLogKey}.csv".! != 0)
//      return Some(s"Error moving $srcPath to ${locationsLocal("ba")}")
//    println(s"=== Finished moving $srcPath to hdfs and archive to ${locationsLocal("ba")} ===")
//3.    Spark process to find any matching existing rows in warehouse and append them to archive table (table iii) adding current run date time as updated_dt

    //val rfrshNm = getRfrshNbr(spark, sc, "ib", s"${fileType}_inbnd", s"${fileType}\\.(.+)\\.csv".r)
    val dfNew = spark.table(s"${dbNms("ib")}${fileType}_inbnd").repartition(numPart)
    val edfNewCast = Spec.cast(spark, dfNew, spec)
    if (edfNewCast.isLeft) {
      sb.append(s"""Error casting data from inbound due to: ${edfNewCast.left.get}""")
    } else {
      // cast success
      val dfNewCast = edfNewCast.right.get
      val eValidate = Spec.validate(spark, dfNewCast, spec)
      if (eValidate.isLeft) {
        sb.append(s"""Column validation failed: ${eValidate.left.get}""")
      } else {
        cnt = eValidate.right.get
        sb.append(s"=== Finished casting and validating new rows from ${fileType}_inbnd ===")
//4.    Write validated data to destinataion
        val dfAdded = dfNewCast.withColumn("load_log_key", lit(logKey.toLong)).withColumn("load_dt", tsCurrent)
          .select("load_log_key", "load_dt" +: dfNewCast.columns:_*)
        val targetPath = s"${locations("wh")}${fileType}_hist/rfrsh_nbr=${Current.loadNbr}"
        sb.append("=== Added columns load_log_key and load_dt ===")
        dfAdded.printSchema
        dfAdded.write.mode(SaveMode.Overwrite).parquet(targetPath)

        val viewCreateQuery = s"""CREATE VIEW ${dbNms("wh")}${fileType} as select * from ${dbNms("wh")}${fileType}_hist where rfrsh_nbr='${Current.loadNbr}'"""
        spark.sql(s"msck repair table ${dbNms("wh")}${fileType}_hist")
        spark.sql(s"drop view if exists ${dbNms("wh")}${fileType}")
        spark.sql(viewCreateQuery)
        js = JobStatus.Success
      }
    }
    println(sb.toString)
    return logExecution(AuditLog(logKey, Current.loadNbr, ara, Array(filepath), fileType, start, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime),  cnt, 0, sb.toString, "B", js, usrNm))
  }
}
