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
 * Parameters: [path to file spec file] [number of partition] [timestamp format]
 */
class Import(env:String, org:String, ara:String, conf:Map[String, String], spark:SparkSession) extends ETLStrategy(env, conf, spark, ara, null, Array()) with ETLTrait {

  override def execute(args: Array[String]): AuditLog = {
    println(s"Execution starts with arguments ${args.mkString(",")}")
    if (args.length < 3) throw new MissingArgumentException(s"Required parameters missing: [path to file spec file] [number of partition] [timestamp format]")
    return execute(args(0), args(1).toInt, args(2))
  }

  def execute(filepath:String, numPart:Int, tsFormat:String): AuditLog = {
    val start = Calendar.getInstance.getTime
    val tsStart = new java.sql.Timestamp(start.getTime)
    val sb = new StringBuilder
    var js = JobStatus.Failure
    var cnt:Long = -1

    val spec = Spec.loadFileSpec(spark, dbNms("stage"), filepath, true, true, tsFormat)
    val fileType = spec.name
    import spark.implicits._
    //val rfrshNm = getRfrshNbr(spark, sc, "inbound", s"${fileType}_in", s"${fileType}\\.(.+)\\.csv".r)
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
        cnt = eValidate.right.get
        sb.append(s"=== Finished casting and validating new rows from ${fileType}_in ===")
//4.    Write validated data to destinataion
        val targetPath = s"${locations("stage")}${fileType}_hst/load_id=${Current.loadId}/load_log_key=${logKey}"
        sb.append("=== Added columns load_log_key and load_ts ===")
        dfNewCast.printSchema
        dfNewCast.write.mode(SaveMode.Overwrite).parquet(targetPath)

        spark.sql(s"msck repair table ${dbNms("stage")}${fileType}_hst")
        spark.sql(s"drop view if exists ${dbNms("stage")}${fileType}")
        spark.sql(s"""CREATE VIEW ${dbNms("stage")}${fileType} as select * from ${dbNms("stage")}${fileType}_hst where load_id='${Current.loadId}' and load_log_key='${logKey}'""")
        js = JobStatus.Success
        archive("inbound", s"${fileType}_in")
        archive("stage", s"${fileType}_hst")
      }
    }
    println(sb.toString)
    return logExecution(AuditLog(logKey, Current.loadId, ara, Array(filepath), fileType, tsStart, new java.sql.Timestamp(Calendar.getInstance.getTime.getTime),  cnt, 0, sb.toString, "B", js, usrNm))
  }
}
