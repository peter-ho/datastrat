USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_${hivevar:db};

DROP TABLE IF EXISTS playerlogonsummary_ou;

CREATE EXTERNAL TABLE IF NOT EXISTS playerlogonsummary_ou (
`player_id` string COMMENT 'id of a player',
`logon_secs` string COMMENT 'number of second a player logged across all games for a given month'
) PARTITIONED BY (`load_id` string, `load_log_key` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/${hivevar:db}/playerlogonsummary_ou'
TBLPROPERTIES ('skip.header.line.count'="1");
