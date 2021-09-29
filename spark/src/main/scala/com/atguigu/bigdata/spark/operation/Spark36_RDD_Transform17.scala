package com.atguigu.bigdata.spark.operation

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark36_RDD_Transform17 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO Scala - 转换算子 - 分区器
    val rdd = sc.makeRDD(
      List(
        ("nba", "xxx"),
        ("cba", "xxx"),
        ("nba", "yyy"),
        ("wnba", "yyy")
      ),
      2
    )

    // 实现自定义分区
    val rdd2 = rdd.partitionBy(new MyPartitioner(2))

    val rdd1 = rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          d => {
            (index, d)
          }
        )
      }
    )

    // println(rdd1.collect().mkString(","))
    rdd1.collect().foreach(println)




    sc.stop
  }

  // 自定义分区器
  // 1. 继承Partitioner分区器
  // 2. 重写方法：numPartitions，getPartition
  class MyPartitioner(num: Int) extends Partitioner{

    // 设定分区的数量
    def numPartitions: Int = num

    // 根据key计算数据所在的分区索引
    def getPartition(key: Any): Int = {
      if (key.isInstanceOf[String]) {
        val keystring = key.asInstanceOf[String]
        if (keystring == "nba") {
          0
        } else {
          1
        }
      } else {
        1
      }
    }

  }






}
