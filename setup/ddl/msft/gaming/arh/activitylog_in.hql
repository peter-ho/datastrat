USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_arh;

SET hive.exec.compress.output=true;

DROP TABLE IF EXISTS activitylog_in;

CREATE EXTERNAL TABLE IF NOT EXISTS activitylog_in (
`activity_ts` string COMMENT 'timestamp of this activity',
`player_id` string COMMENT 'id of a player',
`game_id` string COMMENT 'id of the game played', 
`type` string COMMENT 'type of the activity including LOGON, LOGOFF, WIN, LOSS, START, MOVE',
`params` string COMMENT 'paramters specific to this specific activity'
) 
PARTITIONED BY (`load_id` string, `load_log_key` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
'escapeChar'='\\',
'quoteChar'='\"',
'separatorChar'=',',
'skip.header.line.count'="1"
)
STORED AS TEXTFILE
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/arh/activitylog_in';

