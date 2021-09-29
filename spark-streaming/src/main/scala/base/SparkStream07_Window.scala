package base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Window  有状态转换算子
  */
object SparkStream07_Window {
  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置检查点目录，用于保存状态
    // ssc.checkpoint("cp")

    // 3. 从指定的端口读取数据
    val lineDS = ssc.socketTextStream("hadoop106", 9999)

//    val windowDS = lineDS.window(Seconds(6), Seconds(3))
//
//    val flatMapDS = windowDS.flatMap(_.split(" "))
//
//    val mapDS = flatMapDS.map((_, 1))
//
//    val reduceDS = mapDS.reduceByKey(_+_)

    // reduceByWindow
//    val reduceDS = lineDS
    ////      .flatMap(_.split(" "))
    ////      .map(_.toInt)
    ////      .reduceByWindow(_ + _, Seconds(6), Seconds(3))

    // reduceByKeyAndWindow
//    val reduceDS = lineDS
//      .flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKeyAndWindow((a: Int, b: Int) => {
//        a + b
//      }, Seconds(6), Seconds(3))

    val reduceDS = lineDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => {
          a + b
        },
        (a: Int, b: Int) => {
          a - b
        },
        Seconds(6),
        Seconds(3))
    reduceDS

    reduceDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
