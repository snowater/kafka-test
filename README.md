# kafka-test
1、去官网下载kafka
我下载的版本是 kafka_2.11-0.10.0.1.tgz，下面的实例也是基于该版本。
2、解压安装
tar -xzf kafka_2.11-0.10.0.1.tgz 
mv kafka_2.11-0.10.0.1    /root
3、修改配置文件
cd /root/kafka_2.11-0.10.0.1/config
cp server.properties server1.properties 
cp server.properties server2.properties 
cp server.properties server3.properties 
修改配置中的三个参数如下：
server1.properties
broker.id=1
listeners=PLAINTEXT://:9092
log.dirs=/tmp/kafka-logs-1
server2.properties
broker.id=2
listeners=PLAINTEXT://:9094
log.dirs=/tmp/kafka-logs-2

server3.properties
broker.id=3
listeners=PLAINTEXT://:9094
log.dirs=/tmp/kafka-logs-3
4、启动脚本kafkacluster编写
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 3s
bin/kafka-server-start.sh config/server1.properties &
bin/kafka-server-start.sh config/server2.properties &
bin/kafka-server-start.sh config/server3.properties &
启动kafka集群只需要执行./kafkacluster
5、创建topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic test
