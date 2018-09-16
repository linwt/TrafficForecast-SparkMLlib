package main

import java.text.DecimalFormat
import java.util
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random

object Producer {

  def main(args: Array[String]): Unit = {
    // 读取kafka配置信息
    val props = PropertyUtil.properties
    // 创建生产者对象
    val producer = new KafkaProducer[String, String](props)
    // 定义开始时间
    var startTime = Calendar.getInstance().getTimeInMillis / 1000
    // 模拟生产实施数据，每5分钟切换一次速度。单位：秒。实测用2秒切换一次速度效果更明显
    val trafficCycle = 2

    while(true) {
      // 模拟产生监测点，模拟20个监测点id
      val randomMonitorId = new DecimalFormat("0000").format(new Random().nextInt(21))
      // 定义一个车速
      var randomSpeed = ""
      // 数据模拟堵车切换周期：5分钟
      val currentTime = Calendar.getInstance().getTimeInMillis / 1000
      // 超过5分钟，速度为[0,15]
      if(currentTime - startTime > trafficCycle) {
        randomSpeed = new DecimalFormat("000").format(new Random().nextInt(16))
        if(currentTime - startTime > trafficCycle * 2) {
          startTime = currentTime
        }
      } else {        // 未超过5分钟，速度为[30,60]
        randomSpeed = new DecimalFormat("000").format(new Random().nextInt(31) + 30)
      }
//      println(randomMonitorId + "," + randomSpeed)

      // 把数据放进map
      val jsonMap = new util.HashMap[String, String]()
      jsonMap.put("monitor_id", randomMonitorId)
      jsonMap.put("speed", randomSpeed)
      // 序列化Json
      val event = JSON.toJSON(jsonMap)
      println(event)
      // 发送数据
      producer.send(new ProducerRecord(props.getProperty("kafka.topics"), event.toString))

      Thread.sleep(500)
    }
  }
}

