package main

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}

object SparkConsumer {
  def main(args: Array[String]): Unit = {

    // 初始化spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumer")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    // 设置kafka参数
    val kafkaParam = Map[String, String]("metadata.broker.list" -> PropertyUtil.getProperties("metadata.broker.list"))
    // 设置主题
    val topic = Set(PropertyUtil.getProperties("kafka.topics"))
    // 读取kafka中的value数据（key是topic）
    val kafkaLineDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topic).map(_._2)

    // 解析读取到的数据。{"monitor_id":"0001", "speed":"50"}
    val event = kafkaLineDStream.map(line => {
      // Json解析,解析成JavaMap类型，Map("monitor_id":"0001", "speed":"50")
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]](){})
      // 将JavaMap转为ScalaMap，Map(monitor_id -> 0001, speed -> 50)
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })

    // 将数据进行简单聚合：tuple
    // 目前数据：Map(monitor_id -> 0001, speed -> 50)
    // 目标数据：（0001，（500,10））     （id，（车辆速度和，车辆数））
    val sumOfSpeedCount = event.map(e => (e.get("monitor_id").get, e.get("speed").get))   // ("0001","50")
      .mapValues(v => (v.toInt, 1))     // ("0001", (50,1))
      .reduceByKeyAndWindow((t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2), Seconds(20), Seconds(10))

    // 将处理好的数据存放于redis中
    val dbIndex = 1
    sumOfSpeedCount.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        partition.filter((tuple: (String, (Int, Int))) => tuple._2._2 > 0)
          .foreach(record => {

            val monitorId = record._1   // "0001"
            val sumOfSpeed = record._2._1   // 500
            val sumOfCarCount = record._2._2    // 10
            // 将数据实时保存到reids中
            val currentTime = Calendar.getInstance().getTime
            // HHmm
            val hmSDF = new SimpleDateFormat("HHmm")
            // yyyyMMdd
            val dateDSF = new SimpleDateFormat("yyyyMMdd")
            // 对时间进行格式化
            val hourMinuteTime = hmSDF.format(currentTime)    // "2035"
            val date = dateDSF.format(currentTime)    // "20190115"

            val jedis = RedisUtil.pool.getResource
            // 选择数据库
            jedis.select(dbIndex)
            // key --> "20190115_0012"
            // fields --> 2035
            // value --> 500_10
            jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)
            // 回收资源
            RedisUtil.pool.returnResource(jedis)
//            jedis.close()
          })
      })
    })

    ssc.start
    ssc.awaitTermination()
  }
}
