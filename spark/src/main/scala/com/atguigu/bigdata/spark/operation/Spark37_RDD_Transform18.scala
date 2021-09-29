package com.atguigu.bigdata.spark.operation

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark37_RDD_Transform18 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO Scala - 转换算子 - reduceByKey
    val rdd = sc.makeRDD(
      List(
        ("hello", 1),
        ("hello", 2),
        ("hadoop", 2)
      )
    )

    // val rdd1 = sc.makeRDD(List(1, 2, 3, 4))

    // TODO spark中所有的byKey算子都需要通过KV类型的RDD进行调用
    // reduceByKey = 分组 + 聚合
    // 分组操作已经由Spark自动完成，按照key进行分组。然后在数据的value进行两两聚合
    val rdd1 = rdd.reduceByKey(_+_)

    rdd1.collect().foreach(println)



    sc.stop
  }

}
