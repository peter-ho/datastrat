package com.datastrat.etl

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.commons.cli.MissingArgumentException
import java.util.Calendar
import java.time._
import java.time.format.DateTimeFormatter
import sys.process._
import com.datastrat.etl.ETLStrategy
import com.datastrat.etl.ETLTrait
import com.datastrat.util.Session
import com.datastrat.util.Session._

/**
 * @author Peter Ho
 * @version 1.0
 *
 * Main Spark Launcher for inbound file import
 */

class SCDImport(env:String, org:String, ara:String, conf:Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, ara, null, Array()) with ETLTrait {


  override def execute(args: Array[String]): AuditLog = {
    val numPart = if (args.length > 1) args(1).toInt else 1
    return execute(args(0), numPart)
  }

  def execute(filepath: String, numPart: Int): AuditLog = {
    val log = Logger.getLogger(getClass.getName)
    val start = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    val tsCurrent = current_timestamp()
    val sb = new StringBuilder
    val js = JobStatus.Failure
    var cnt:Long = -1

    val spec = Spec.loadFileSpec(spark, dbNms("wh"), filepath, true, true, "", "")
    val fileType = spec.name
    val targetPath = s"${locations("wh")}/$fileType"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._
//1.	New text delimited bot files are loaded in Linux fs on edge node
//2.	process to load from Linux fs to HDFS inbound directory/hive table (table i) overwriting existing files and archiving the incoming files
//    val srcPath = s"${locationsLocal("bt")}${fileType}.csv"
//    if (!java.nio.file.Paths.get(srcPath).toFile().exists())
//      return Some(s"Source file $srcPath not found")
//    fs.copyFromLocalFile(new Path(srcPath), new Path(s"${locations("ib")}${fileType}_inbnd"))
//    s"mkdir -p ${locationsLocal("ba")}".!
//    if (s"mv $srcPath ${locationsLocal("ba")}${fileType}-${LoadLogKey}.csv".! != 0) 
//      return Some(s"Error moving $srcPath to ${locationsLocal("ba")}")
//    println(s"=== Finished moving $srcPath to hdfs and archive to ${locationsLocal("ba")} ===")
//3.	Spark process to find any matching existing rows in warehouse and append them to archive table (table iii) adding current run date time as updated_dt
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
        sb.append(s"=== Finished casting and validating new rows from ${fileType}_inbnd ===")
        cnt = eValidate.right.get
// 4.	Spark to append conflicted rows in hist table with updt_dt as current timestamp
        val dfCurrent =  spark.table(s"${dbNms("wh")}${fileType}")
        val colKeys = spec.columns.filter(_.isKey).map(x => x.name)
        // identify and filter rows with the same key and add current date time as updt_dt
        val dfHist1 = dfCurrent.join(dfNewCast.select(colKeys.head, colKeys.tail:_*), colKeys).withColumn("updt_dt", lit(tsCurrent))
        val dfHist = dfHist1.select("updt_dt", dfHist1.columns.filter(_ != "updt_dt"):_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val cntHist = dfHist.count
        dfHist.write.mode(SaveMode.Append).parquet(s"${locations("wh")}${fileType}_hist")
        logExecution(AuditLog(logKey, Current.loadNbr, ara, Array(filepath), fileType + "_hist", start, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), cntHist, 0, sb.toString, "B", js, usrNm))
        sb.append(s"=== Finished appending ($cntHist) conflicting rows to ${fileType}_hist ===")
//5.	Spark to append current run date time as load_dt to new data and union with existing non-conflicting rows
        val colNew = Array("load_log_key", "load_dt") ++ dfNewCast.columns
        val dfNewCurrent = dfNewCast.withColumn("load_log_key", lit(logKey.toLong))
          .withColumn("load_dt", lit(tsCurrent)).select(colNew.head, colNew.tail:_*)
          .unionAll(dfCurrent.select(colNew.head, colNew.tail:_*).except(dfHist.select(colNew.head, colNew.tail:_*)))
         .persist(StorageLevel.MEMORY_AND_DISK_SER)
        cnt = dfNewCurrent.count
        sb.append(s"=== Finished combining current and new with total row count ($cnt) ===")
//6.	Write data from #5 to temp location to avoid reading/writing at the same location
        val tempPath = s"${targetPath}_temp"
        dfNewCurrent.write.mode(SaveMode.Overwrite).parquet(tempPath)
        dfNewCurrent.unpersist
        dfHist.unpersist
        sb.append(s"=== Finished writing data to $tempPath ===")
//7.	Replace current table (table ii) with temp location in #6
        fs.delete(new Path(targetPath), true)
        fs.rename(new Path(tempPath), new Path(targetPath))
        sb.append(s"=== Finished moving $tempPath to $targetPath ===")
      }
    }
    println(sb.toString)
    return logExecution(AuditLog(logKey, Current.loadNbr, ara, Array(filepath), fileType, start, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), cnt, 0, sb.toString, "B", js, usrNm))
  }
}
