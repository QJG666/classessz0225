package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark25_RDD_Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO : 将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
    val wordRDD = sc.makeRDD(
      List("Hello", "hive", "hbase", "Hadoop"),
      2
    )

    val groupRDD = wordRDD.groupBy(
      word => {
        // word.substring(0, 1)
        // word.charAt(0)
        word(0)
      }
    )

//    groupRDD.collect().foreach(println)
    groupRDD.saveAsTextFile("output")


    sc.stop

  }

}
