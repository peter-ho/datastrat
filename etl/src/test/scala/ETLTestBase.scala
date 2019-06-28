package com.datastrat.test

import java.sql.Timestamp
import java.io.File
import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, Column, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
//import org.scalatest._

object ETLTestBase { 

  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

  lazy val spark = SparkSession
      .builder()
      .master("local")
      .appName("testLocal")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //.config.setSparkHome("~/sparkhome")
      .config("spark.eventLog.dir", "~/sparkhome/log")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.ui.port", (7700 + Math.abs(scala.util.Random.nextInt(1000))).toString)
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  lazy val sparkContext = spark.sparkContext
  lazy val LoadLogKey = "testLoadLogKey12345"
  lazy val defaultRfrshNbr:String = "201811"
  lazy val defaultArgs = Array("default")
  lazy val conf = Map(
    "load.nbr" -> "201906272030000", 
    "db.inbound" -> "",
    "db.stage" -> "",
    "db.work" -> "",
    "db.warehouse" -> "",
    "db.outbound" -> "",
    "db.archive" -> "",

    "pth.inbound" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/inbound/",
    "pth.stage" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/stage/",
    "pth.work" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/work/",
    "pth.reference" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/reference/",
    "pth.warehouse" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/warehouse/",
    "pth.outbound" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/outbound/",
    "pth.archive" -> "hdfs://quickstart.cloudera/dev/msft/data/gaming/r001/archive/"
  )

  lazy val resourcesDirectory = new File("src/test/resources")
  def compare(expected: DataFrame, actual: DataFrame): Boolean = {
    import spark.implicits._
    actual.show(60, false)
    //println("actual: " + actual.columns)
    //println("expected: " + expected.columns)
    val dCompare = expected.join(actual,
        expected.schema.filter(_.dataType.typeName != "array").map(_.name)
          .map(x => trim(expected(x)) <=> trim(actual(x)))
          .reduce(_ and _), "outer")
        .filter(expected.schema.filter(_.dataType.typeName == "array").map(_.name)
          .map(x => actual(x) === expected(x))
          .reduceOption(_ and _).getOrElse(lit(true)))
    println(s"counts: ${actual.count} ${dCompare.count}")
    dCompare.show(60, false)
    expected.count() == actual.count && dCompare.count() == expected.count
  }

  def castDf(data: RDD[String], schema: Array[(String, String)]) = {
    import spark.implicits._
    var d = data.toDF("data").filter(not('data.startsWith(schema(0)._1)))
      .withColumn("data", split('data, ","))
    List.range(0, schema.size).foreach(x => d = d.withColumn(schema(x)._1, 'data.getItem(x).cast(schema(x)._2)))
    d
  }

  def cast(data: DataFrame, schema: Array[(String, String)]) = {
    var d = data
    List.range(0, schema.size).foreach(x => d = d.withColumn(schema(x)._1, d(schema(x)._1).cast(schema(x)._2)))
    d
  }

  def loadTbl(nm: String, schema: Array[(String, String)], subarea: String) = {
    val path = s"${resourcesDirectory.getAbsolutePath()}/$subarea/$nm.csv"
    val d = cast(spark.read.option("header", true).csv(path), schema)
    d.show(3, false)
    d.createOrReplaceTempView(nm)
    println(s"""Loaded table $nm from $path""")
    spark.table(nm).show(3, false)
  }

  def loadTbl(nm: String, subarea: String) = {
    val pathSch = s"${resourcesDirectory.getAbsolutePath()}/$subarea/$nm.schema"
    val path = s"${resourcesDirectory.getAbsolutePath()}/$subarea/$nm.csv"
    val txtSch = Source.fromFile(pathSch).getLines.toArray.map(_.split(" ").filter(_.length > 1))
    val schema = txtSch.filter(_(1).toLowerCase != "string").map(x => (x(0).replace("`", ""), x(1)))
    val d = cast(spark.read.option("header", true).csv(path), schema)
    d.show(3, false)
    d.createOrReplaceTempView(nm)
    println(s"""Loaded table $nm from $path""")
    spark.table(nm).show(3, false)
  }

  loadTbl("ara", "bot")
}
