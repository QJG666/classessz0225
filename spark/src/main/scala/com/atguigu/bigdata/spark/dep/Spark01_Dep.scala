package com.atguigu.bigdata.spark.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Dep {

  def main(args: Array[String]): Unit = {

    // TODO Spark - 依赖关系
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    // 一个SparkContext就是一个Application
    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile("input")
    println("************ file ***************")
    // println(fileRDD.toDebugString)
    println(fileRDD.dependencies)

    val wordRDD = fileRDD.flatMap(_.split(" "))
    println("************ flatMap ***************")
    // println(wordRDD.toDebugString)
    println(wordRDD.dependencies)

    val word2OneRDD = wordRDD.map((_, 1))
    println("************ map ***************")
    // println(word2OneRDD.toDebugString)
    println(word2OneRDD.dependencies)

    val word2IterRDD = word2OneRDD.groupBy(_._1)
    println("************ groupBy ***************")
    // println(word2IterRDD.toDebugString)
    println(word2IterRDD.dependencies)

    val word2CountRDD = word2IterRDD.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println("************ map ***************")
    // println(word2CountRDD.toDebugString)
    println(word2CountRDD.dependencies)

    // 一个行动算子，就会产生一个job
    val result = word2CountRDD.collect()
    // result.foreach(println)

    sc.stop()

  }

}
