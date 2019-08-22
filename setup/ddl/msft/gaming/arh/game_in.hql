USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_arh;

SET hive.exec.compress.output=true;

DROP TABLE IF EXISTS game_in;

CREATE EXTERNAL TABLE IF NOT EXISTS game_in (
`id` string COMMENT 'id of the game played',
`name` string COMMENT 'name of the game',
`type` string COMMENT 'type of the game including action, action-adventure, adventure, role-playing, simulation, strategy, vehicle simulation',
`list_price` decimal(5,2) COMMENT 'list price of the game'
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
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/arh/game_in';

