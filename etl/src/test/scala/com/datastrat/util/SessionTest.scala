package com.datastrat.test

import com.datastrat.util.SessionInstance
import java.sql.Timestamp

class SessionTest extends org.scalatest.FunSuite {
  test("util.Session") {
    val s = SessionInstance(new Timestamp(2019 - 1900, 6, 25, 10, 32, 22, 4))
    assert(s.yyEnd === 2019)
    assert(s.yyPrv === 2018)
    assert(s.ymEnd === "201907")
    assert(s.ymStrt === "201606")
    assert(s.ymYtd === Array("201901", "201902", "201903", "201904", "201905", "201906", "201907"))
    assert(s.ymMap("q1") === Array("201901", "201902", "201903"))
    assert(s.ymMap("pq2") === Array("201804", "201805", "201806"))
  }
}
