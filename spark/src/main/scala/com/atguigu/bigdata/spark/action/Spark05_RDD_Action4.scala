package com.atguigu.bigdata.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Action4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark - RDD - 行动算子
    val rdd = sc.makeRDD(List("Hello", "world", "Hello", "Scala"), 1)

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.map((_, 1)).saveAsSequenceFile("output3")


    sc.stop()
  }

}
