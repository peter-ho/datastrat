package com.datastrat.etl

import org.apache.spark.sql.DataFrame

/**
 *
 * @author Peter Ho
 * @version  1.0.0
 * 
 */

/** Result of an execution capturing information to be logged
 */
case class ExeResult (comment: String, data: Option[DataFrame], load_type: String, row_count:Long = -1) {
  def this(result:ExeResult, data:Option[DataFrame], row_count:Long) = this(result.comment, data, result.load_type, row_count)
}
object ExeResult {
  def apply(result:ExeResult, data:Option[DataFrame], row_count:Long) = new ExeResult(result, data, row_count)
}

/** Status of a job at the end of its execution, implicitly casted as String
 */
object JobStatus extends Enumeration { type JobStatus = Value ; val Success, Failure = Value ; implicit def enum2Str(x:JobStatus) = x.toString }

/** Log entry of an execution that matches schema in the audit_log table
 */
case class AuditLog (load_log_key:String, load_id:String, ara_nm:String, src_tbl_nms:Array[String], tgt_tbl_nm:String, load_strt_ts:java.sql.Timestamp, load_end_ts:java.sql.Timestamp, trgt_cnt:Long, trgt_orig_cnt:Long, comment:String, load_type:String, status:String, load_by:String)

/** A generic interface for Extract Transform Load implementations
 */
trait ETLTrait {
  def execute(args: Array[String]): AuditLog { }
}
