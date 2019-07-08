USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_inb;

DROP TABLE IF EXISTS activitylog;

CREATE EXTERNAL TABLE IF NOT EXISTS activitylog (
`activity_ts` string COMMENT `timestamp of this activity`,
`player_id` string COMMENT `id of a player`,
`game_id` string COMMENT `id of the game played`, 
`type` string COMMENT `type of the activity including LOGON, LOGOFF, WIN, LOSS, START, MOVE`,
`params` string COMMENT `paramters specific to this specific activity`
) PARTITIONED BY (`load_id` string, `load_log_key` string)
STORED AS PARQUET
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/inb/activitylog;

