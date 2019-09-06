hdfs dfs -rm -r /dev/msft/data/gaming/v01/*/*
./run_dev_bee.sh setup/ddl/msft/gaming/arh/activitylog_in.hql
./run_dev_bee.sh setup/ddl/msft/gaming/arh/game_in.hql
./run_dev_bee.sh setup/ddl/msft/gaming/inb/activitylog_in.hql
./run_dev_bee.sh setup/ddl/msft/gaming/inb/game_in.hql
./run_dev_bee.sh setup/ddl/msft/gaming/stg/activitylog_hst.hql
./run_dev_bee.sh setup/ddl/msft/gaming/stg/game_hst.hql
./run_dev_bee.sh setup/ddl/msft/gaming/stg/game.hql
./run_dev_bee.sh setup/ddl/msft/gaming/whs/playerlogonsummary_hst.hql
./run_dev_bee.sh setup/ddl/msft/gaming/oub/playerlogonsummary_ou.hql
./run_dev_bee_arh.sh setup/ddl/msft/gaming/oub/playerlogonsummary_ou.hql

