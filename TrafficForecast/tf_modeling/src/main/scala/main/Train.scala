package main

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

object Train {
  def main(args: Array[String]): Unit = {
    // 将本次评估结果保存到文件中
    val writer = new PrintWriter(new File("model_training.txt"))

    //设置spark
    val conf = new SparkConf().setMaster("local[2]").setAppName("TrafficTrain")
    val sc = new SparkContext(conf)

    // redis相关
    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    // 设置想要对哪个监测点（卡口）进行数据建模
    val monitorIDs = List("0005", "0015")
    // 对上面两个监测点进行建模，但是这两个监测点可能需要其他监测点的数据信息
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017")
    )
    // 遍历上面所有的监测点，进行建模
    monitorIDs.map(monitorID => {   // 0005, 0015
      // "0003", "0004", "0005", "0006", "0007"
      val monitorRelationList = monitorRelations.get(monitorID).get
      // 处理时间
      val currentDate = Calendar.getInstance().getTime
      // 设置时间格式化
      val hourMinuteSDF = new SimpleDateFormat("HHmm")
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      val dateOfString = dateSDF.format(currentDate)

      // 使用“相关监测点”，取得当日的所有的监测点的平均车速
      // 最终结果样式：（0005，（1033=2000_100， 1034=1000_40））    (时间=速度和_车辆和)
      val relationInfo = monitorRelationList.map(monitorID => (monitorID, jedis.hgetAll(dateOfString + "_" + monitorID)))

      // 使用n小时内的数据进行建模
      val hours = 1
      // 用于存放特征向量和特征结果的映射关系
      val dataTrain = ArrayBuffer[LabeledPoint]()
      // 用于存放特征向量
      val dataX = ArrayBuffer[Double]()
      // 用于存放Label向量
      val dataY = ArrayBuffer[Double]()

      // 将时间拉回1小时之前，单位：分钟
      for(i <- Range(60 * hours, 2, -1)) {
        dataX.clear()
        dataY.clear()
        // 以下内容包含：线性滤波
        for(index <- 0 to 2) {
          // 第3分钟（从0开始）作为Label向量
          val oneMoment = currentDate.getTime - 60 * i * 1000 + 60 * index * 1000
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))

          // 取得该时刻下里面的数据
          // 取出的数据形式举例：（0005， {1033=93_2, 1034=1354_30}）
          for((k, v) <- relationInfo) {
            // 如果index == 2，则前3分钟的数据已经组装到dataX，则下一刻的数据，如果是目标卡口，则需要存放于dataY中
            if(k == monitorID && index == 2) {
              // 第四分钟数据
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(nextMoment))

              // 判断是否有数据
              if(v.containsKey(nextHM)) {
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat  // 得到第四分钟的平均车速
                dataY += valueY
              }
            }
            // 组装前3分钟的dataX
            if(v.containsKey(oneHM)) {
              val speedAndCarCount = v.get(oneHM).split("_")
              val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
              dataX += valueX
            } else {
              dataX += -1.0F
            }
          }
        }
        // 训练模型
        // 先将dataX和dataY映射于一个LabelPoint对象中
        if(dataY.toArray.length == 1) {
          val label = dataY.toArray.head    // 答案的平均车速
          // label的取值范围时：0~15， 30~60 --> 0,1,2,3,4,5,6,7,8,9
          // 真实情况：0~120km/h车速，划分10个级别，公式如下：
          val record = LabeledPoint(if(label.toInt / 10 < 10) label.toInt / 10 else 10, Vectors.dense(dataX.toArray))
          dataTrain += record
        }
      }
      // 将数据集写入到文件中方便查看
      dataTrain.foreach(record => {
        println(record)
        writer.write(record.toString()+ "\n")
      })
      // 开始组装训练集和测试集
      val rddData = sc.parallelize(dataTrain)

      // 切分数据集
      val randomSplits = rddData.randomSplit(Array(0.6, 0.4), 11L)
      // 训练集
      val trainingData = randomSplits(0)
      // 测试集
      val testData = randomSplits(1)

      if(!rddData.isEmpty()) {
        // 使用训练集进行建模
        val model = new LogisticRegressionWithLBFGS().setNumClasses(11).run(trainingData)
        // 完成建模后，使用测试集，评估模型精确度
        val predictionAndLabels = testData.map {
          case LabeledPoint(label, features) =>
            val prediction = model.predict(features)
            (prediction, label)
        }
        // 得到当前监测点model的评估值
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val accuracy = metrics.accuracy     // 取值范围：0.0~1.0
        println("评估值：" + accuracy)
        writer.write(accuracy.toString + "\r\n")

        // 设置评估阈值：超过多少精确度，则保存模型
        if(accuracy > 0.4) {
          // 将模型保存到hdfs
          val hdfsPath = "hdfs://mini1:9000/traffic/model" + monitorID + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(currentDate)
          model.save(sc, hdfsPath)
          jedis.hset("model", monitorID, hdfsPath)
        }
      }
    })
    RedisUtil.pool.returnResource(jedis)
    writer.flush()
    writer.close()
  }
}
