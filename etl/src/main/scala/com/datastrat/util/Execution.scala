/*
 * Copyright (c) 2019, All rights reserved.
 *
 */
package com.datastrat.util

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author Peter Ho
 */
object Execution {
  def retry(exe:()=>Boolean, msg: String, retryCnt:Integer=3): String = {
    val sb = new StringBuilder
    var tryCount = 1
    while (tryCount < retryCnt && !exe()) {
      sb.append(s"$msg at try $tryCount\n")
      tryCount += 1
    }
    sb.toString
  }

  def fillExcpMsg(sb:StringBuilder, e:Throwable):Unit = {
    sb.append(e.toString).append(e.getStackTraceString).append("\n")
    if (e.getCause != null) fillExcpMsg(sb, e.getCause)
  }

  /** Get file parts by regex
    *
    * @example 1
    *          {{{HadoopFileUtil.getFilenames("/user/hive/testrename/wk_clm_rndrg_prov", s"file_type\\.(.+)\\.csv".r)}}}
    * @param hdfsDirPath          : hdfs dir name
    * @param rx                   : regular expression for extracting parts of filenames
    */
  def getFilenameMatch(hdfsDirPath:String, rx:scala.util.matching.Regex) : Array[scala.util.matching.Regex.Match] = {
    getFilenames(hdfsDirPath).map(rx.findFirstMatchIn(_)).filter(_ != None).map(_.get)
  }

  /** Get filenames from  HDFS directory.
    *
    * @example 1
    *          {{{HadoopFileUtil.getFilenames("/user/hive/testrename/wk_clm_rndrg_prov")}}}
    * @param hdfsDirPath         : Source hdfs dir
    */
  def getFilenames(hdfsDirPath:String) : Array[String] = {
    val fs = FileSystem.get(Session.hadoopConfiguration)
    val status = fs.listStatus(new Path(hdfsDirPath))
    status.filter(x => x.isFile()).map(x => x.getPath().getName())
  }
}
