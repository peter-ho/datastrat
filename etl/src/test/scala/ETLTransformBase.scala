package com.datastrat.etl

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, Column, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
//import com.typesafe.config.Config
//import com.typesafe.config.ConfigFactory
import java.sql.Timestamp
import java.io.File
import org.scalatest._

//abstract class ETLTransformBase extends FlatSpec { 
object TestBase { 

  System.setProperty("hadoop.home.dir", "c:\\winutil\\")
  val conf: SparkConf = { val conf = new SparkConf()
    conf.setAppName("testLocal")
    conf.setMaster("local")
    conf.setSparkHome("~/sparkhome")
    conf.set("spark.eventLog.dir", "~/sparkhome/log")
    conf.set("spark.sql.shuffle.partitions", "5")
    conf.set("spark.ui.port", (7700 + Math.abs(scala.util.Random.nextInt(1000))).toString)
  }
  
  lazy val sc = new SparkContext(conf)
  //lazy val envSetting = ConfigFactory.load("local_application")
  lazy val sqlContext = new HiveContext(sc)
  lazy val dbNames: Map[String, String] = Map("wk" -> "", "sg" -> "", "wh" -> "")
  lazy val resourcesDirectory = new File("src/test/resources");
  //val dbNs: Map[String, String] = Map("wk" -> "dv_ebphqx1ph_allob_r000_wk.", "sg" -> "dv_ebphqx1ph_allob_r000_sg.", "wh" -> "dv_ebphqx1ph_allob_r000_wh.")
  def compare(expected: DataFrame, actual: DataFrame): Boolean = {
    import sqlContext.implicits._
    actual.show(60, false)
    //println("actual: " + actual.columns)
    //println("expected: " + expected.columns)
    val dCompare = expected.join(actual,
        expected.columns.map(x => trim(expected(x)) <=> trim(actual(x)))
          .reduce((x,y) => x and y), "outer")
    println(s"counts: ${actual.count} ${dCompare.count}")
    dCompare.show(60, false)
    expected.count() == actual.count && dCompare.count() == expected.count
  }

  def castDf(data: RDD[String], schema: Array[(String, String)]) = {
    import sqlContext.implicits._
    var d = data.toDF("data").filter(not('data.startsWith(schema(0)._1)))
      .withColumn("data", split('data, ","))
    List.range(0, schema.size).foreach(x => d = d.withColumn(schema(x)._1, 'data.getItem(x).cast(schema(x)._2)))
    d
  }

  def loadTbl(nm: String, schema: Array[(String, String)], path: String) = {
    val d = castDf(sc.textFile(resourcesDirectory.getAbsolutePath() + path, 1), schema)
    d.show(3, false)
    d.registerTempTable(nm)
    println(s"""Loaded table $nm from $path""")
  }

  //def setupBaseTables(): Unit = {
    import sqlContext.implicits._

    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
    val dEn = Seq(
       ("CSWT9"   ,"2014-01-23 00:00:00.0","2014-01-25 00:00:00.0")
      ,("CSWT9"   ,"2014-01-23 04:00:00.0","2014-01-26 00:00:00.0")
      ,("CSWT9"   ,"2014-02-24 00:00:00.0","2014-03-23 00:00:00.0")
      ,("CSWT9"   ,"2014-03-24 23:59:59.9","2014-05-23 00:00:00.0")
      ).map(t => (t._1.asInstanceOf[String], new Timestamp(fmt.parse(t._2).getTime()), new Timestamp(fmt.parse(t._3).getTime()))) 
      .toDF("id", "start_dt", "end_dt").registerTempTable("test_en")

    val hqx_rfrsh_hdr_sch = Array(("rfrsh_nbr", "string"), ("start_dt", "timestamp"), ("end_dt", "timestamp"), ("adjctn_dt", "timestamp"), ("load_dt", "timestamp"))
    loadTbl("rfrsh_hdr", hqx_rfrsh_hdr_sch, "/outbound/hqx_rfrsh_hdr.csv")
} 
