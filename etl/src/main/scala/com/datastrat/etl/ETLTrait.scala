package com.datastrat.etl
import org.apache.spark.sql.DataFrame
/**
 *
 * @author Peter Ho
 * @version  1.0.0
 * 
 */

/** Result of an extraction capturing information to be logged
 */
case class ExtractResult (comment: String, data: Option[DataFrame], src_tbl_nms:  Array[String], load_type: String) {
  def this(result:ExtractResult, data:Option[DataFrame]) = this(result.comment, data, result.src_tbl_nms, result.load_type)
}

/** Status of a job at the end of its execution, implicitly casted as String
 */
object JobStatus extends Enumeration { type JobStatus = Value ; val Success, Failure = Value ; implicit def enum2Str(x:JobStatus) = x.toString }

/** Log entry of an execution that matches schema in the audit_log table
 */
case class AuditLog (load_log_key: String, load_id:String, subj_area_nm:String, src_tbl_nms: Array[String], tgt_tbl_nm: String, load_strt_dtm: java.sql.Timestamp, load_end_dtm: java.sql.Timestamp, trgt_cnt: Long, trgt_orig_cnt: Long, comment: String, load_type: String, status: String, load_by: String)

/** A generic interface for Extract Transform Load implementations
 */
trait ETLTrait {
  def extract(args: Array[String]): AuditLog { }
}
