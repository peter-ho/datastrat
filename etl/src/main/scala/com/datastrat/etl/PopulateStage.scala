package com.datastrat.etl
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import com.datastrat.util.SqlExt._

/**
 * @author Peter Ho
 * @version 1.0
 *
 * An abstract class for populating a table in stage database
 * For rows with multiple values, uniqueId can be added for specifying the rows to be selected 
 * with _1 being the sequence of columns to be identified as unique
 * and _2 being the sequence of columns for sorting with the first columns being picked 
 * for the result
 */
abstract class PopulateStage(env:String, conf:Map[String,String], spark:SparkSession, araNm:String,
  tgtTbl:String, srcTbl:(String,String), idCrt: (Seq[Column], Seq[Column]) =null)
  extends ETLStrategy(env, conf, spark, araNm, ("stage", tgtTbl), Array(srcTbl)) {

  override def executeInternal(args: Array[String]): ExeResult = {
    import spark.implicits._
    var df = spark.table(tn(srcTbl))

    if (idCrt != null) {
      val overDf = df.withColumn("rc", row_number().over(getWindowExpr(idCrt._1, idCrt._2)))
      val latestDf = overDf.where("rc = 1")
      df = latestDf.drop("rc")
    }

    ExeResult(null, Some(df), "InboundToStage")
  }
}
