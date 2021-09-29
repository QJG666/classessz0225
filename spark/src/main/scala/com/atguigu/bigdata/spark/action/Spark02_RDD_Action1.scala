package com.atguigu.bigdata.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Action1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark - RDD - 行动算子
    // 这里的行动的概念指的是，让当前应用程序开始执行
    val rdd = sc.makeRDD(
      List(3, 1, 4, 2)
    )
    // 1234 => 12
    // 31 => 13

   // TODO
    val ints = rdd.takeOrdered(2)
    println(ints.mkString(","))


    sc.stop()
  }

}
