package com.atguigu.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求二实现
  * 需求：统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）
  * 窗口大小：     1h
  * 窗口滑动步长：6s
  * yyyy-MM-dd hh:mm:ss
  * 统计的内容：msg = 1584271384370,华南,广州,100,1 ==> 1584271384370, 1 ==> hhmm, 1
  * 最终数据形式：(adid, hhmm), 1
  */
object RealTimeApp2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置checkpoint
    ssc.checkpoint("output1")

    //kafka参数声明
    val brokers = "hadoop105:9092,hadoop106:9092,hadoop107:9092"
    val topic = "ad_log_0225"
    val group = "myGroup"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      //位置策略，指定计算的Executor
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))

    //测试Kafka中消费数据
    val resDS: DStream[String] = kafkaDS.map(_.value())

    // TODO 需求：统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）
    // 定义窗口数据
    // 因为测试：6秒相当于窗口大小1小时
    val windowDS = resDS.window(Seconds(6), Seconds(3))

    val sdf = new SimpleDateFormat("hh:mm")

    // 从kafka中读取到的数据进行结构的转换
    val mapDS = windowDS.map(
      line => {
        val fields = line.split(",")

        (fields(4) + "_" + sdf.format(new Date(fields(0).toLong)), 1)
      }
    )

    mapDS.reduceByKey(_+_).print()



    //打印输出
   // resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

