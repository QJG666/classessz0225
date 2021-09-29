package base

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream08_Ouput {
  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // ssc.sparkContext.longAccumulator
    // ssc.sparkContext.broadcast()

    // 设置检查点目录，用于保存状态
    // ssc.checkpoint("cp")

    // 3. 从指定的端口读取数据
    val lineDS = ssc.socketTextStream("hadoop106", 9999)

//    lineDS
//      .flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_+_)
//      .saveAsTextFiles("output/atguigu", "fangfang")

    val reduceDS = lineDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // 通过sql操作DS

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    reduceDS.foreachRDD(
      rdd => {
        val df = rdd.toDF("word", "count")
        df.createOrReplaceTempView("test")
        spark.sql("select * from test").show
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
