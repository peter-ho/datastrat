/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
class CountValleyTest extends org.scalatest.FunSuite {
  test("CountValley.countValley") {
    assert(CountValley.countingValleys(0, "") === 0)
    assert(CountValley.countingValleys(1, "D") === 0)
    assert(CountValley.countingValleys(1, "U") === 0)
    assert(CountValley.countingValleys(8, "UDDDUDUU") === 1)
    assert(CountValley.countingValleys(8, "DUDUDUDU") === 4)
    assert(CountValley.countingValleys(8, "UUUUDDDD") === 0)
    assert(CountValley.countingValleys(8, "UDUDUDUD") === 0)
  }
}