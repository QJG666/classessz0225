package com.atguigu.bigdata.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Action5 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark - RDD - 行动算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    rdd.collect().foreach(println)    // 内存循环打印
    println("***************************")
    // 算子
    // Driver
    rdd.foreach(
      str => {
        // Executor
        println(str)  // 分布式循环打印
      }
    )


    sc.stop()
  }

}
