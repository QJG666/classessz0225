package com.atguigu.bigdata.spark.operation

import org.apache.spark.{SparkConf, SparkContext}

object Spark48_RDD_Transform26 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO Scala - 转换算子 - leftOuterJoin
    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("a", 3)
      )
    )

    val rdd2 = sc.makeRDD(
      List(
        ("a", 4), ("b", 5), ("a", 6)
      )
    )

    // co = connect => 同一个RDD中相同的key连接在一起
    // group = 组 => 不同RDD中相同的key放置在一个组中
    val result = rdd1.cogroup(rdd2)

    result.collect().foreach(println)

    sc.stop
  }

}
