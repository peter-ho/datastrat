package com.datastrat.util

import java.io.InputStream
import java.util.Properties
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.datastrat.util.Session._

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
    val mp: HashMap[String,String] = HashMap.empty[String,String]
    try {
      val confBasePth = s"/$env/$org/etl/config/*.properties"
      val confPth = s"/$env/$org/etl/config/$ara/*.properties"
      //TODO: potential to utilize resource packaged in jar utilizing ClassLoader.getSystemResourceAsStream("application.properties")
      val fs = FileSystem.get(hadoopConfiguration)
      val load = (p:String) => 
        fs.globStatus(new Path(p))
          .filter(x => x.isFile && x.getLen > 0).map(_.getPath)
          .foreach(x => loadConfFromFile(env, org, ara, fs, x, mp))
      load(confBasePth)
      load(confPth)
    } catch {
      case e:Exception => e.printStackTrace()
    }
    return mp
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
    * @param mp hash map with configuration entries to be replaced by the configuration file
    * @return accumulated map with key value pair that matches configuration entries within the given configuration file replacing any existing entries with the same key
    */
  def loadConfFromFile(env:String, org:String, ara:String, fs:FileSystem, filepth:Path, mp:HashMap[String, String]):HashMap[String,String] = {
    try {
      println(s" ... reading from configuration file in hdfs: $filepth")

      val props = loadFromFile(fs, filepth)
      if (props != null) {
        props.stringPropertyNames().asScala.toList.foreach(
          propName => mp.put(propName, props.getProperty(propName)
            .replace("$env", env).replace("$org", org).replace("$ara", ara)
            + (if (propName.startsWith("db-")) "." else "")))
      }
    } catch {
      case e:Exception => e.printStackTrace()
    }
    mp
  }

  /** Reads a java properties file to a [[java.util.Properties]]
    * @example 1
    *          {{{loadFromFile("hdfs:///dev/org1/app1/config/app.properties")}}}
    * @param fs FileSystem associated to the current Hadoop/Spark instance
    * @param filepth absolute path of the properties file to be read
    * @return an instance of [[java.util.Properties]] with the key value pairs in the properties file
    */
  def loadFromFile(fs:FileSystem, pth:Path):Properties = {
    var input:InputStream = null
    try {
      val fs = FileSystem.get(new Configuration())
      val properties = new Properties()
      properties.load(fs.open(pth))
      properties
    } catch {
      case e:Exception => e.printStackTrace(); throw e
    }
  }
}

