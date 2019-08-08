#scp  -r -P 2272 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null setup/* root@192.168.1.227:/home/cloudera/workspace/setup
scp -P 2272 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null etl/target/scala-2.11/data-strat_2.11-*.jar root@192.168.1.227:/home/cloudera/workspace
