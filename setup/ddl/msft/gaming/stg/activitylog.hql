USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_stg;

DROP TABLE IF EXISTS activitylog;

CREATE EXTERNAL TABLE IF NOT EXISTS activitylog (
`activity_ts` timestamp COMMENT `timestamp of this activity`,
`player_id` int COMMENT `id of a player`,
`game_id` int COMMENT `id of the game played`, 
`type` string COMMENT `type of the activity including LOGON, LOGOFF, WIN, LOSS, START, MOVE`,
`params` array<string> COMMENT `paramters specific to this specific activity`
) PARTITIONED BY (`load_id` string, `load_log_key` string)
STORED AS PARQUET
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/stg/activitylog;

