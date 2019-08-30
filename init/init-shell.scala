import sys.process._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, Column, SparkSession}
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.{FileSystem, Path}

val dbPfx = s"dev_msft_v01_"
val dbNms = Map(
      "inbound" -> s"${dbPfx}inb.",
      "stage" -> s"${dbPfx}stg.",
      "work" -> s"{dbPfx}wrk.",
      "reference" -> s"${dbPfx}ref.",
      "warehouse" -> s"${dbPfx}whs.",
      "outbound" -> s"${dbPfx}oub.",
      "archive" -> s"${dbPfx}arh.")

def tn(tblNm:(String, String)):String = s"${dbNms(tblNm._1)}${tblNm._2}"
