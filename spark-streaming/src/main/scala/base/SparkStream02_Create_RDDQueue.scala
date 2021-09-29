package base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 通过RDD队列创建DStream
  */
object SparkStream02_Create_RDDQueue {
  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // 3.1 创建RDD队列
    val queue = new mutable.Queue[RDD[Int]]()

    // 3.2 创建离散化流
    // 第二个参数：oneAtTime，一个采集周期是否只处理一个RDD，默认为true
    val queueDS = ssc.queueStream(queue, false)

    // 4. 对采集到的DS的数据进行操作
    queueDS.map((_, 1)).reduceByKey(_+_).print

    // 5. 开启采集线程采集数据
    ssc.start()

    // 6. 向队列中放数据
    for ( i <- 1 to 5 ) {
      // 通过sssc获取SparkContext，创建RDD，并将创建好的RDD放到RDD队列中
      queue.enqueue(ssc.sparkContext.makeRDD(1 to 5))
      Thread.sleep(2000)
    }





    // 等待采集结束，终止上下文对象
    ssc.awaitTermination()

  }
}
