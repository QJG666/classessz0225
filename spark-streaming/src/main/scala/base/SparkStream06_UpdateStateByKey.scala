package base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * UpdateStateByKey  有状态转换算子
  */
object SparkStream06_UpdateStateByKey {
  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置检查点目录，用于保存状态
    ssc.checkpoint("cp")

    // 3. 从指定的端口读取数据
    val lineDS = ssc.socketTextStream("hadoop106", 9999)

    val flatMapDS = lineDS.flatMap(_.split(" "))

    val mapDS = flatMapDS.map((_, 1))

    // reduceByKey只能对当前采集周期的数据进行聚合
    // val reduceDS = mapDS.reduceByKey(_+_)

    // updateStateByKey   记录上个采集周期的状态，和当前采集周期的数据进行聚合
    val resDS = mapDS.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(state.getOrElse(0) + seq.sum)
      }
    )

    resDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
