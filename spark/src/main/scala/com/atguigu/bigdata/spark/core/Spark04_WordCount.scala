package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount {

  def main(args: Array[String]): Unit = {

    // Spark 程序分为几步?
    // TODO 1. 获取spark的连接对象(上下文环境对象)
    // 创建Spark的配置对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 2. 通过Spark环境对象操作数据

    // TODO 2.1 通过Spark环境对象读取文件中的数据
    // Spark读取文件时可以指定路径名称，这样会将这个路径下所有的文件读取
    // 将文件中数据一行一行的读取出来
    val fileRDD = sc.textFile("data/word.txt")

    // TODO 2.2 将文件中的数据进行拆分（分词）（扁平化）
    val wordRDD = fileRDD.flatMap(_.split(" "))

    // TODO 2.3 将拆分后的数据进行结构的改变
    // word => (word, 1)
    val word2OneRDD = wordRDD.map((_, 1))

    // TODO 2.4-5 Spark作为一个计算框架可以将其中的计算过程进行简化和整合
    // TODO 2.4 将转换结构后的数据根据单词进行分组
    // TODO 2.5 将分组后的数据进行聚合
    // reduceByKey: 根据数据的key进行简化，规约
    //              相同的key的数据，让value进行简化，规约
    //              将分组和聚合功能融合在一起
    val word2CountRDD = word2OneRDD.reduceByKey(_ + _)

    // TODO 2.6 将聚合结果展示在控制台上
    val result = word2CountRDD.collect()
    result.foreach(println)

    // TODO 3. 释放连接
    sc.stop()


  }

}
