hdfs dfs -mkdir -p /dev/msft/etl/config/gaming
hdfs dfs -rm /dev/msft/etl/config/application.properties
hdfs dfs -rm /dev/msft/etl/config/gaming/gaming.properties
hdfs dfs -put config/application.properties /dev/msft/etl/config
hdfs dfs -put config/msft/gaming/gaming.properties /dev/msft/etl/config/gaming
