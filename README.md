# TrafficForecast
SparkMLlib智慧交通项目
## 项目需求
对已有交通数据进行分析建立模型，从而对未来交通堵车情况进行预测
## 使用步骤
1. 克隆项目到本地
2. 导入项目到IDEA
3. linux中安装配置各软件
4. 阅读代码，运行项目
## 软件版本
1. hadoop-2.6.4 
2. zookeeper-3.4.5
3. kafka_2.12-0.11.0.2
4. jdk-8u181-linux-i586
5. redis-2.6.16
## 项目思路
### 生产者模块
- 功能：模拟生产数据，发送到kafka，kafka接收数据后打印到控制台
- 操作步骤
1. 启动zookeeper(三台机器）
> [hadoop@mini1 ~]$ zkServer.sh start
2. 启动hadoop
> [hadoop@mini1 ~]$ start-all.sh
3. 启动kafka(三台机器）
> [hadoop@mini1 kafka_2.12-0.11.0.2]$ bin/kafka-server-start.sh  config/server.properties
4. 创建topic
> [hadoop@mini1 kafka_2.12-0.11.0.2]$ bin/kafka-topics.sh \
--create \
--zookeeper mini1:2181 \
--replication-factor 1 \
--partitions 1 \
--topic traffic
5. 启动consumer
> [hadoop@mini1 kafka_2.12-0.11.0.2]$ bin/kafka-console-consumer.sh \
--zookeeper mini1:2181 \
--topic traffic \
--from-beginning
6. 运行程序Producer
### 消费者模块
- 功能：消费kafka数据，并将处理后的数据存储到Redis中
- 操作步骤
1. 启动Redis
> 服务端：[hadoop@mini1 redis]# bin/redis-server ~/apps/redis/etc/redis.conf \
> 客户端：[hadoop@mini1 redis]# bin/redis-cli
2. 运行程序SparkConsumer
3. 查看Redis数据库
> 127.0.0.1:6379> select 1 \
> 127.0.0.1:6379[1]> keys *	\
> 127.0.0.1:6379[1]> hgetall “20180412_0001”
### 数据建模模块
- 功能：读取Redis数据库数据，进行数据建模，并将模型保存到hdfs
- 操作步骤
1. 运行程序Train
2. web访问hdfs，查看保存结果
> mini1:50070
