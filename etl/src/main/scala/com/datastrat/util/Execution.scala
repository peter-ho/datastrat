/*
 * Copyright (c) 2019, All rights reserved.
 *
 */
package com.datastrat.util

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

}
