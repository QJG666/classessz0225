package base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


/**
  * SparkStreaming入门案例
  */
object SparkStream01_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkStreaming程序执行的入口
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreamingDemo")
    val ssc = new StreamingContext(conf, Seconds(3))

    // TODO 读取指定端口获取数据，并且数据是以流的形式，源源不断的过来
    val lineDS = ssc.socketTextStream("hadoop106", 9999)

    // TODO 对当前读到的一行数据进行扁平映射
    val flatMapDS = lineDS.flatMap(_.split(" "))

    //val rdd = ssc.sparkContext.makeRDD(List(1, 2, 3))

    // TODO 对数据进行结构的转换
    val mapDS = flatMapDS.map((_, 1))

    // TODO 对单词出现的次数进行计数
    val reduceDS = mapDS.reduceByKey(_+_)

    reduceDS.print

    // 开始采集
    ssc.start()


    // 释放资源
    // 因为要实时不间断的采集数据，所以不能调用stop方法结束StreamingContext
    // ssc.stop()

    // 让采集线程一直执行
    ssc.awaitTermination()
  }

}
