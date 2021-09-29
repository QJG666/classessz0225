package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 从文件中创建RDD
    // master => spark环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // 读取文件数据，读取文件数据时一般是一行一行的读取
    // 底层采用Hadoop的读取文件的方式
    // 读取文件的路径可以是本地路径，也可以是分布式存储路径
    val fileRDD = sc.textFile("input")
    println(fileRDD.collect().mkString(","))


    sc.stop

  }

}
