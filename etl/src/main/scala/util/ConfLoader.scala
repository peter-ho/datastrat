package com.datastrat.util

import java.io.InputStream
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._

object ConfLoader {

  /** Read configuration file(s) set up within the application given a specified environment, organization and area
    * @example 1
    *          {{{apply("dev", "msft", "user.activity")}}}
    * @param env environment identifier: dev, tst, uat, prd
    * @param org organization id identifing a tenant to be separated with others
    * @param ara subject area of the etl configuration
    * @return map with key value pair that matches configuration entries within the given configuration file
    */
  def apply(env: String, org: String, ara: String):HashMap[String,String] = {
    val mapp: HashMap[String,String] = HashMap.empty[String,String]
    try {
      val confBasePth = s"/$env/$org/etl/config/*.properties"
      val confPth = s"/$env/$org/etl/config/$ara/*.properties"

//ClassLoader.getSystemResourceAsStream("application.properties")

      val fs = FileSystem.get(getHadoopConfiguration)
      val load = (p:String, m:HashMap[String,String]) => fs.globStatus(new Path(p))
        .filter(x => x.isFile && x.getLen > 0).map(_.getPath)
        .foreach(x => {
          val props = loadFromFile(x)
          if (props != null) {
            props.stringPropertyNames().asScala.toList.foreach(
            propName => m.put(propName, props.getProperty(propName)
              .replace("$env", env).replace("$org", org).replace("$ara", ara)
              + (if (propName.startsWith("db.")) "." else "")))
          }
        }
      load(
    } catch {
      case e:Exception => e.printStackTrace()
    }
    return mapp
  }

  /** Read configuration file with a given file path resolving the entries with the given envirnoment, 
    * organization, area
    * @example 1
    *          {{{loadConfFromFile("dev", "msft", "user.activity", "/dev/msft/etl/config/user.activity/app.properties")}}}
    * @param env environment identifier: dev, tst, uat, prd
    * @param org organization id identifing a tenant to be separated with others
    * @param ara subject area of the etl configuration
    * @param fs FileSystem associated to the current Hadoop/Spark instance
    * @param filepth absolute path of the configuration file to be loaded
    * @return map with key value pair that matches configuration entries within the given configuration file
    */
  def loadConfFromFile(env:String, org:String, ara:String, fs:FileSystem, filepth:Path):HashMap[String,String] = {
    val mapp: HashMap[String,String] = HashMap.empty[String,String]
    try {
      println(s" ... reading from configuration file in hdfs: $filepth")

      val props = loadFromFile(filepth)
      if (props != null) {
        props.stringPropertyNames().asScala.toList.foreach(
          propName => mapp.put(propName, props.getProperty(propName)
            .replace("$env", env).replace("$org", org).replace("$ara", ara)
            + (if (propName.startsWith("db-")) "." else "")))
      }
    } catch {
      case e:Exception => e.printStackTrace()
    }
    return mapp
  }

  /** Reads a java properties file to a [[java.util.Properties]]
    * @example 1
    *          {{{loadFromFile("hdfs:///dev/org1/app1/config/app.properties")}}}
    * @param fs FileSystem associated to the current Hadoop/Spark instance
    * @param filepth absolute path of the properties file to be read
    * @return an instance of [[java.util.Properties]] with the key value pairs in the properties file
    */
  def loadFromFile(fs:FileSystem, filepth:Path):Properties = {
    var input:InputStream = null
    try {
      val pth = new Path(filepth)
      val fs = FileSystem.get(new Configuration())
      val properties = new Properties()
      properties.load(fs.open(pth))
      properties
    } catch {
      case e:Exception => e.printStackTrace(); throw e
    }
  }
}

