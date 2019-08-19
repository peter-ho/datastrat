package com.datastrat.etl

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
      val dvr = args(4)
      val conf = ConfLoader(env, org, ara, dvr)
      //conf.put("load.nbr", if (args.length == 5) "" else args(4))
      println(s" ... execution starts [${args(0)}] ${conf.getOrElse("load.nbr", "")}")
      val stra = cnstr.newInstance(env, org, ara, conf.toMap, spark).asInstanceOf[ETLTrait]
      val al = stra.execute(args.slice(5, args.length))
      println(al)
      if (JobStatus.Success.toString != al.status) {
        sys.exit(1)
      }
    }
  }
}
