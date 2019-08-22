USE ${hivevar:env}_${hivevar:org}_${hivevar:dvr}_stg;

DROP TABLE IF EXISTS game_hst;

CREATE EXTERNAL TABLE IF NOT EXISTS game_hst (
`load_dt` timestamp COMMENT 'timestamp when data is laoded',
`updt_dt` timestamp COMMENT 'timestamp when this row is updated',
`id` string COMMENT 'id of the game played',
`name` string COMMENT 'name of the game',
`type` string COMMENT 'type of the game including action, action-adventure, adventure, role-playing, simulation, strategy, vehicle simulation',
`list_price` decimal(5,2) COMMENT 'list price of the game'
) PARTITIONED BY (`load_id` string, `load_log_key` string)
STORED AS PARQUET
LOCATION '/${hivevar:env}/${hivevar:org}/data/gaming/${hivevar:dvr}/stg/game_hst';

