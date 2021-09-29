package com.atguigu.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * 用于测试从Kafka中消费数据
  */
object RealTimeApp {
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

    //==========需求一实现： 每天每地区热门广告   msg = 1584271384370,华南,广州,100,1==========
    //1.对获取到的Kafka中传递的原始数据

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    //2.将原始数据转换结构  (天_地区_广告,点击次数)
    val mapDS = resDS.map(
      line => {
        val fields = line.split(",")
        // 获取日期
        val date = new Date(fields(0).toLong)
        // 对日期进行格式化 获取天
        val day = sdf.format(date)

        val area = fields(1)
        val ad = fields(4)

        (day + "_" + area + "_" + ad, 1)
      }
    )

    // 对某天某地区某广告的点击次数进行聚合
    // updateStateByKey，会保留历史采集周期中数据状态
    // status，历史采集周期状态---计算结果
    // seq，当前采集周期中，每个key对应的value

    // (天_地区_广告, SumCount)
    val reduceDS = mapDS
      .updateStateByKey(
        (seq: Seq[Int], status: Option[Int]) => {
          // 汇总数据
          Option(seq.sum + status.getOrElse(0))
        }
      )

    // reduceDS

    // 转换结构 ---> (天_地区, (广告, count))
    val day_areaDS = reduceDS.map {
      case (day_area_adid, sumcount) => {
        val infos = day_area_adid.split("_")
        (infos(0) + "_" + infos(1), (infos(2), sumcount))
      }
    }
    //day_areaDS

    // 按照天和地区进行分组   (天_地区, Iter[(广告A, count), (广告B, count), (广告C, count)])
    val groupDS = day_areaDS.groupByKey()

    // 对分组后的数据，按照广告点击次数降序排序   取前3
    val resDS1 = groupDS
      .mapValues(
        _.toList.sortBy(-_._2).take(3)
      )


    //打印输出
    resDS1.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

