USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_${hivevar:db};

DROP TABLE IF EXISTS playerlogonsummary_hst;

CREATE EXTERNAL TABLE IF NOT EXISTS playerlogonsummary_hst (
`month_id` string COMMENT 'id of the current month in yyyyMM format',
`player_id` int COMMENT 'id of a player',
`logon_secs` int COMMENT 'number of second a player logged across all games for a given month'
) PARTITIONED BY (`load_id` string, `load_log_key` string)
STORED AS PARQUET
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/${hivevar:db}/playerlogonsummary_hst';

