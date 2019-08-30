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
    println(s"Execution starts with arguments ${args.mkString(",")}")
    if (args.length < 3) throw new MissingArgumentException(s"Required parameters missing: [path to file spec file] [number of partition] [timestamp format] [load id format]")
    return execute(args(0), args(1).toInt, args(2), args(3))
  }

  def execute(filepath: String, numPart: Int, tsFormat:String, loadIdFmt:String): AuditLog = {
    val log = Logger.getLogger(getClass.getName)
    val start = Calendar.getInstance.getTime
    val tsStart = new java.sql.Timestamp(start.getTime)
    val tsCurrent = current_timestamp()
    val sb = new StringBuilder
    var js = JobStatus.Failure
    var cnt:Long = -1

    val spec = Spec.loadFileSpec(spark, dbNms("stage"), filepath, true, true, tsFormat)
    val fileType = spec.name
    //val targetPath = s"${locations("stage")}${fileType}_hst"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._
//1.	New text delimited bot files are loaded in Linux fs on edge node
//2.	process to load from Linux fs to HDFS inbound directory/hive table (table i) overwriting existing files and archiving the incoming files
//    val srcPath = s"${locationsLocal("bt")}${fileType}.csv"
//    if (!java.nio.file.Paths.get(srcPath).toFile().exists())
//      return Some(s"Source file $srcPath not found")
//    fs.copyFromLocalFile(new Path(srcPath), new Path(s"${locations("ib")}${fileType}_in"))
//    s"mkdir -p ${locationsLocal("ba")}".!
//    if (s"mv $srcPath ${locationsLocal("ba")}${fileType}-${LoadLogKey}.csv".! != 0) 
//      return Some(s"Error moving $srcPath to ${locationsLocal("ba")}")
//    println(s"=== Finished moving $srcPath to hdfs and archive to ${locationsLocal("ba")} ===")
//3.	Spark process to find any matching existing rows in warehouse and append them to archive table (table iii) adding current run date time as updated_ts
    updateCurrent(loadIdFmt, "inbound", s"${fileType}_in", s"${fileType}\\.(.+)\\.csv".r)
    val dfNew = spark.table(s"${dbNms("inbound")}${fileType}_in").repartition(numPart)
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
        sb.append(s"=== Finished casting and validating new rows from ${fileType}_in ===")
        cnt = eValidate.right.get
// 4.	Spark to append conflicted rows in hst table with updt_ts as current timestamp
        var df:DataFrame = dfNewCast.withColumn("load_ts", tsCurrent).withColumn("updt_ts", tsCurrent).select("load_ts", ("updt_ts" +: dfNewCast.columns):_*)
        val currTblNm = s"${dbNms("stage")}${fileType}"
        if (spark.catalog.tableExists(currTblNm)) {
          val dfCurrent =  spark.table(s"${dbNms("stage")}${fileType}")
          val colKeys = spec.columns.filter(_.isKey).map(x => x.name)
          // identify and filter rows with the same key and add current date time as updt_ts
          dfNewCast.show
          dfCurrent.show
          val dfHst1 = dfCurrent.join(dfNewCast.select(colKeys.head, colKeys.tail:_*), colKeys).withColumn("updt_ts", tsCurrent)
          dfHst1.show
          val colHst = Array("updt_ts", "load_log_key") ++ dfNewCast.columns
          val dfHst = dfHst1.select("load_ts", colHst:_*).persist(StorageLevel.MEMORY_AND_DISK_SER)
          dfHst.show
          val cntHst = dfHst.count
          dfHst.write.mode(SaveMode.Append).parquet(s"${locations("stage")}${fileType}_hst")
          logExecution(AuditLog(logKey, Current.loadId, ara, Array(filepath), fileType + "_hst", tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), cntHst, 0, sb.toString, "B", js, usrNm))
          sb.append(s"=== Finished appending ($cntHst) conflicting rows to ${fileType}_hst ===")
//5.	Spark to append current run date time as load_ts to new data and union with existing non-conflicting rows
          val colNew = Array("load_ts", "load_log_key") ++ dfNewCast.columns
          val dfNewCurrent = dfNewCast.withColumn("load_log_key", lit(logKey))
            .withColumn("load_ts", lit(tsCurrent)).select(colNew.head, colNew.tail:_*)
            .union(dfCurrent.select(colNew.head, colNew.tail:_*).except(dfHst.select(colNew.head, colNew.tail:_*)))
           .persist(StorageLevel.MEMORY_AND_DISK_SER)
          dfHst.unpersist
          cnt = dfNewCurrent.count
          df = dfNewCurrent
        }
        val targetPath = s"${locations("stage")}${fileType}"
        sb.append(s"=== Finished combining current and new with total row count ($cnt) ===")
//6.	Write data from #5 to temp location to avoid reading/writing at the same location
        val tempPath = s"${targetPath}_temp"
        df.write.mode(SaveMode.Overwrite).parquet(tempPath)
        df.unpersist
        sb.append(s"=== Finished writing data to $tempPath ===")
//7.	Replace current table (table ii) with temp location in #6
        fs.delete(new Path(targetPath), true)
        fs.rename(new Path(tempPath), new Path(targetPath))
        sb.append(s"=== Finished moving $tempPath to $targetPath ===")

/*
        sb.append("=== Added columns load_log_key and load_ts ===")
        df.printSchema
        df.write.mode(SaveMode.Overwrite).parquet(targetPath)
*/
        js = JobStatus.Success
        archive("inbound", s"${fileType}_in")
      }
    }
    println(sb.toString)
    return logExecution(AuditLog(logKey, Current.loadId, ara, Array(filepath), fileType, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime), cnt, 0, sb.toString, "B", js, usrNm))
  }
}
