package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO : 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val list = List(1, 4, 2, 8)

    val rdd = sc.makeRDD(list, 2)

    // 将分区数据封装到数组当中
    val glomRDD = rdd.glom()

    // 将封装后的数组进行结构的转换 array => max
    val maxRDD = glomRDD.map(
      array => array.max
    )

    // 将数据采集回来进行统计  (求和)
    val ints = maxRDD.collect()
    println(ints.sum)


    sc.stop

  }

}
