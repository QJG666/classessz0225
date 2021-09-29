package base

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取数据
  */
object SparkStream05__Kafka10_Direct {
  def main(args: Array[String]): Unit = {
    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    //3. 构建Kafka参数
    val kafkaParmas: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop105:9092,hadoop106:9092,hadoop107:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "myGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 4. 读取Kafka数据，创建DS
    val kafkaDS = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("bigdata0225"), kafkaParmas)
    )

    kafkaDS
      .map(_.value())
      .flatMap(_.split(" "))
      .map(( _, 1 ))
      .reduceByKey(_ + _)
      .print()

    ssc.start()

    ssc.awaitTermination()
  }
}
