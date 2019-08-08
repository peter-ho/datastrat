USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_whs;

DROP TABLE IF EXISTS audit_load;

CREATE EXTERNAL TABLE IF NOT EXISTS `audit_load`(
  `load_log_key` string,
  `load_id` string,
  `ara_nm` string,
  `src_tbl_nms` array<string>,
  `tgt_tbl_nm` strng,
  `load_strt_dtm` timestamp,
  `load_end_dtm` timestamp,
  `trgt_cnt` long,
  `trgt_orig_cnt` long,
  `comment` string,
  `load_type` string,
  `status` string,
  `load_by` string)
STORED AS TEXTFILE
LOCATION '/${hivevar:env}/${hivevar:org}/data/system/${hivevar:dvr}/whs/audit_load;

