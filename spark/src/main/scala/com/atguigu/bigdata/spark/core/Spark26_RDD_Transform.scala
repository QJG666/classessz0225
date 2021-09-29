package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark26_RDD_Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    val numRDD = sc.makeRDD(
      List(1, 2, 3, 4)
    )

    // TODO Scala - 转换算子 - filter
    val filterRDD = numRDD.filter(num => num % 2 == 0)

    filterRDD.collect().foreach(println)


    sc.stop

  }

}
