USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_whs;

DROP TABLE IF EXISTS player_logon_summary;

CREATE EXTERNAL TABLE IF NOT EXISTS player_logon_summary (
`month_id` string COMMENT `id of the current month in yyyyMM format`,
`player_id` int COMMENT `id of a player`,
`logon_secs` int COMMENT `number of second a player logged across all games for a given month`
) PARTITIONED BY (`load_id` string, `load_log_key` string)
STORED AS PARQUET
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/warehouse/player_logon_summary;

