package com.atguigu.bigdata.spark.broadcast

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Broadcast {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("broadcast").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      )
    )

    val rdd2 = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("c", 6)
      )
    )

    // ("a", (1, 4), ("b", (2, 5), ("c", (3, 6)
    val joinRDD = rdd1.join(rdd2)

    joinRDD.foreach(println)




    sc.stop()
  }

}
