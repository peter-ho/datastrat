beeline -n developer -u jdbc:hive2://localhost:10000 -f $1 --hivevar "env=dev" --hivevar "org=msft" --hivevar "ara=gaming" --hivevar "dvr=v01" --hivevar "db=arh"
