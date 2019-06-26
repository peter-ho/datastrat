/*
 * Copyright (c) 2019, All rights reserved.
 *
 */
package com.datastrat.util

import java.util.{Date, Calendar, Properties}
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

/**
 * @author Peter Ho
 * Extension of DataFrame to ease the usage of DataFrame
 */
object SqlExt {

  val udfMnthGen = udf((strt:java.sql.Timestamp, end:java.sql.Timestamp) =>
    (0 to (12*(end.getYear - strt.getYear)+end.getMonth-strt.getMonth) toArray)
      .map(x => ((1900 + strt.getYear + (x+strt.getMonth)/12) * 100 + (x+strt.getMonth)%12+1).toString))

  /** Get the [[WindowSpec]] instance for windowing functions based on partition columns and order by columns
    *
    * @return an instance of [[WindowSpec]] supporting the provided partitionBy and orderBy columns
    */
  def getWindowExpr(partitionCols: Seq[Column], orderStmnt: Seq[Column]): WindowSpec = {
    Window.partitionBy(cols=partitionCols:_*).orderBy(cols=orderStmnt:_*)
  }

  implicit class DataFrameExt(df: DataFrame) {
    /** based on df, the appnd [[DataFrame*]] is union to the end, with any missing column(s) added as null
     *
     * @param appnd a sequence of [[DataFrame]] instance to be appended to the base [[DataFrame]] instance
     * @return an instance of [[org.apache.spark.sql.DataFrame]] with base and appnd union together
     */
    def union(appnd:DataFrame*):DataFrame = {
      df.union(appnd.map(x => x.select((
        df.columns.intersect(x.columns).map(x(_)) 
        ++ df.columns.diff(x.columns).map(lit(null).as(_))):_*)
          .select(df.columns.head, df.columns.tail:_*)).reduce((x,y) => x.union(y)))
    }

  
    /** based on df, write out a list of profiling result of the [[DataFrame]] using show
     *
     * @param rowCnt number of rows to be shown
     * @return the original instance of [[DataFrame]] with base and appnd union together
     */
    def report(rowCnt:Int = 50):DataFrame = {
      val cols = df.columns
      cols.foreach(x => {
        df.groupBy(x).agg(count(x).as("cnt")).orderBy(desc("cnt")).show(rowCnt, false)
      })
      df
    }

    /** based on df, write out debugging information in standard out if debugging is turned on
     *
     * @param namen name of the [[DataFrame]] to be identified in output
     * @return the original instance of [[DataFrame]] 
     */
    def debug(name:String):DataFrame = {
      ///TODO: if debug is on
      df.cache
      println(s"$name count: ${df.count}")
      df.show(150, false)
      df
    }

    /** replace a given list of columns by its own value within a partition
      *
      * @example 1
      *          {{{replaceWith(Seq("val"), Seq(d("id")), Seq(desc("year")))}}}
      * @param colsToReplace name of columns to be replaced
      * @param prtBy list of columns to be used for partition
      * @param ordBy list of columns to be used for identifying the value to be used for replacement
      * @return a data frame with the same schema and row count but column specified to be replaced with its own based on order and partitioning
      */
    def replaceWith(colsToReplace:Seq[String], prtBy:Seq[Column], ordBy:Seq[Column]):DataFrame = {
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

  }
}
