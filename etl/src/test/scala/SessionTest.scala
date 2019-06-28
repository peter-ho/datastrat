package com.datastrat.test

import com.datastrat.util.SessionInstance
import java.sql.Timestamp

class SessionTest extends org.scalatest.FunSuite {
  test("util.Session") {
    val s = SessionInstance(new Timestamp(2019 - 1900, 6, 25, 10, 32, 22, 4))
    assert(s.yyEnd === 2019)
  }
}
