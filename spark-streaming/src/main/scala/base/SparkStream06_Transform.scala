package base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform无状态转换算子的使用
  */
object SparkStream06_Transform {
  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // 3. 从指定的端口读取数据
    val lineDS = ssc.socketTextStream("hadoop106", 9999)

    val resDS = lineDS.transform(
      rdd => {
        val flatMapRDD = rdd.flatMap(_.split(" "))
        val mapRDD = flatMapRDD.map((_, 1))
        val reduceRDD = mapRDD.reduceByKey(_ + _)
        val sortRDD = reduceRDD.sortByKey()
        sortRDD
      }
    )
    resDS.print

    ssc.start()





    ssc.awaitTermination()
  }
}
