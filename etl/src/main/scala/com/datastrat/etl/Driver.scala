package com.datastrat.etl

/*
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
import org.apache.commons.lang3.RandomUtils
import org.apache.log4j.Logger
import com.datastrat.util.Session._
import com.datastrat.util.SessionInstance
import com.datastrat.util.SqlExt._
import com.datastrat.util.Execution._
*/
import org.apache.commons.cli.MissingArgumentException
import com.datastrat.util.ConfLoader
import com.datastrat.util.Session._

object Driver {
  def main(args: Array[String]) {
    if (args.length < 3) throw new MissingArgumentException("class name, environment (dev, tst, uat, prd) are required as a parameter, organization, and area are required.")
    else {
      val cnstrs  = Class.forName(args(0)).getConstructors()
      if (cnstrs.length == 0) throw new Exception(s"Constructor not found in class ${args(0)}")
      val cnstr = cnstrs(0)
      val env = args(1)
      val org = args(2)
      val ara = args(3)
      val conf = ConfLoader(env, org, ara)
      conf.put("load.nbr", if (args.length == 4) "" else args(4))
      println(s" ... execution starts [${args(0)}] ${conf.getOrElse("load.nbr", "")}")
      val stra = cnstr.newInstance(env, org, ara, conf.toMap, spark).asInstanceOf[ETLTrait]
      val al = stra.execute(args.slice(4, args.length))
      println(al)
      if (JobStatus.Success.toString != al.status) {
        sys.exit(1)
      }
    }
  }
}
