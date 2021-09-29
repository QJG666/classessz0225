package com.atguigu.bigdata.spark.operation

import org.apache.spark.{SparkConf, SparkContext}

object Spark47_RDD_Transform25 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO Scala - 转换算子 - leftOuterJoin
    val rdd1 = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      )
    )

    val rdd2 = sc.makeRDD(
      List(
        ("e", 4), ("d", 5)
      )
    )

    val result = rdd1.leftOuterJoin(rdd2)

    val result1 = rdd2.rightOuterJoin(rdd1)

    result.collect().foreach(println)
    // result1.collect().foreach(println)

    sc.stop
  }

}
