package com.datastrat.etl

import scala.collection.Map
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.unix_timestamp

/**
 *
 * @author Peter Ho
 * @version  1.0
 *
 * A specification for files received from external system
 */
object Spec { 
  val reserved_columns = Array("load_id", "load_log_key", "updt_ts", "load_ts")
  case class ColumnSpec(name: String, typ: DataType, validation: String, isKey: Boolean = false)
  case class FileSpec(name: String, columns: Seq[ColumnSpec], trimString: Boolean, ignoreEmptyRows: Boolean, tsFormat: String, arrayDelimiter:String)

  /** Load inbound file specification from configuration file and Hive
    * TODO: for array, element type should be captured from dt.catalogString = array<string> 
    *   or dt.json = {"type":"array","elementType":"string","containsNull":true}
    *
    * @example 1
    *          {{{Spec.loadFileSpec(spark, "wh", "src/main/resources/hqxinboundfilespec/episodelevelcostbreakdown)
    *          }}}
    * @param spark [[org.apache.spark.sql.SparkSession]] instance for  accessing Hive metadata store data
    * @param databaseName warehouse database name
    * @param filepath relative path of the configuration file to be loaded
    * @param trimString [[scala.Boolean]] true if String columns should be trimmed, false otherwise
    * @param ignoreEmptyRows [[scala.Boolean]] true if empty row should be ignored, false otherwise
    * @param tsFormat [[java.lang.String]] format of timestamp of the input data, default hive format is applied if this is empty
    * @return a new instance of FileSpec loaded from parameters provided
    */
  def loadFileSpec(spark:SparkSession, databaseName:String, filepath: String, trimString: Boolean, ignoreEmptyRows: Boolean, arrayDelimiter:String=",", tsFormat: String = "", tblSuffix: String = "_hst"): FileSpec = {
    val mapValidation = scala.collection.mutable.Map[String, String]()
    var setKeys = Set[String]()
    val path = java.nio.file.Paths.get(filepath)
    val fileType = path.getFileName().toString()
    //val content = Source.fromFile(filepath).getLines()
    val strm = getClass.getResourceAsStream(filepath)
    val content = Source.fromInputStream(strm).getLines()
    content.foreach(x => { 
      val y = x.split("\t") 
      if (y(1).length > 0) mapValidation += (y(0).toLowerCase -> y(1))
      if (y.length > 2 && y(2).length > 0 && y(2) == "k") setKeys += y(0)
    })
    println(s"""Loading: ${databaseName}${fileType}""")
    val df = spark.table(s"${databaseName}${fileType}$tblSuffix")
    FileSpec(fileType, df.schema.filter(x => !reserved_columns.contains(x.name.toLowerCase))
      .map(x => ColumnSpec(x.name, x.dataType, mapValidation.getOrElse(x.name, ""), setKeys.contains(x.name))), trimString, ignoreEmptyRows, tsFormat, arrayDelimiter)
  }

  /** Cast a dataframe according to the data type specified in FileSpec instance passed in
    * @param spark [[org.apache.spark.sql.SparkSession]] instance providing access to implicit functions
    * @param data [[org.apache.spark.sql.DataFrame]] instance with data to be casted
    * @param spec [[com.datastrat.Spec.FileSpec]] instance with file data type specification
    * @return Either an error message or validation failure message is returned in the left 
    *   or a dataframe casted according to the FileSpec data types in the right
    */
  def cast(spark:SparkSession, data:DataFrame, spec:FileSpec): Either[String, DataFrame] = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    var df = data

    /// when there is key columns in spec, remove column header as it's likely source is bot files which contain header
    /// due to spark not supporting skipping header
    val keyCols = spec.columns.filter(x => x.isKey).map(_.name)
    if (keyCols.length > 0) {
      df = df.filter(keyCols.map(x => trim(df(x)).notEqual(x)).reduce((x, y) => (x and y)))
    }

    /// filter out empty rows
    if (spec.ignoreEmptyRows) {
      df = df.filter(not(df.columns.map(x => length(df(x)) === 0).reduce((x,y) => x and y)))
    }

    /// cast non-string columns to new columns with new data type
    spec.columns.filter(!_.typ.equals(org.apache.spark.sql.types.StringType)).foreach(c => {
      val colnameC = c.name + "1"
      val col = df(c.name)
      if (c.typ.equals(org.apache.spark.sql.types.TimestampType) && spec.tsFormat != "") 
        df = df.withColumn(colnameC, unix_timestamp(trim(col), spec.tsFormat).cast("timestamp"))
      else if (c.typ.toString.startsWith("ArrayType"))
        df = df.withColumn(colnameC, split(trim(col), spec.arrayDelimiter))
      else df = df.withColumn(colnameC, trim(col).cast(c.typ))
    })
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /// validate each non-string column not to be null when the original is also not null
    spec.columns.filter(!_.typ.equals(org.apache.spark.sql.types.StringType)).foreach(c => {
      val colnameC = c.name + "1"
      val col = df(c.name)
      val dfNullCheck = df.where( col.isNotNull.and(col.notEqual("")).and(df(colnameC).isNull))
      if (dfNullCheck.count > 0) {
        dfNullCheck.show
        return Left(s"Column $c contains value that results in a null after cast.")
      } else {
        df = df.drop(c.name).withColumnRenamed(colnameC, c.name) // renamed casted column as original name
      }
    })
    /// apply trim to each string column
    if (spec.trimString) {
      spec.columns.filter(x => x.typ.equals(org.apache.spark.sql.types.StringType) && x.name != "rfrsh_nbr").foreach(c => {
        df = df.withColumn(c.name, org.apache.spark.sql.functions.trim(df(c.name)))
      })
    }

    val cols = spec.columns.map(_.name).filter(_ != "rfrsh_nbr")
    df = df.select(cols.head, cols.tail:_*)
    df.printSchema
    Right(df)
  }

  /** Validate a [[org.apache.spark.sql.DataFrame]] according to the data type specified in FileSpec instance passed in
    * @param spark [[org.apache.spark.sql.SparkSession]] instance providing access to implicit functions
    * @param data [[org.apache.spark.sql.DataFrame]] instance with data to be validated
    * @param spec [[com.datastrat.etl.Spec.FileSpec]] instance with file data type specification for validation
    * @return Either an error message or validation failure message is returned in the left 
    *   or the number of rows in the dataframe in the right
    */
  def validate(spark: SparkSession, data: DataFrame, spec: FileSpec): Either[String, Long] = {
    val rowCount = data.count
    spec.columns.filter(_.validation.length > 0).foreach(x => {
      val count = data.filter(data(x.name).rlike(x.validation)).count
      if (count != rowCount) 
        return Left(s"""Column [${x.name}] contains ${rowCount - count} row(s) that failed validation: ${x.validation}""")
    })
    Right(rowCount)
  }
}
