package com.datastrat.test

//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.FlatSpec
//import org.scalatest.BeforeAndAfter
//import org.scalatest.BeforeAndAfterAll
//import java.io.File
import com.datastrat.etl.ETLTestBase._

class Bot extends FlatSpec { 

  import spark.implicits._

  "bot loading" should "load bot files from resources as dataframe" in {
    import spark.implicits._

    val botPrfAttrb = spark.read.option("header", true).csv(resourcesDirectory.getAbsolutePath() + "/msft.bot/ara.csv")
    assert(botPrfAttrb.count > 0)
  }
}
