package com.atguigu.bigdata.spark.operation

import org.apache.spark.{SparkConf, SparkContext}

object Spark32_RDD_Transform13 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)

    // TODO Scala - 转换算子 - sortBy - 排序
    // val rdd = sc.makeRDD(List(1, 3, 5, 2, 4))

    // 按照指定的规则进行排序，默认为升序

    // val sortRDD = rdd.sortBy( num => num)
    // 如果想要降序处理，可以设定第二个参数为false
    // val sortRDD = rdd.sortBy( num => num, false)

    // 排序默认按照指定的顺序来排
    // 数字 => 数字大小
    // 字符串 => 字典顺序
    // 对象 => tuple => 按照顺序来排

    val rdd = sc.makeRDD(
      List(
        User("张三", 30),
        User("李四", 40),
        User("李四", 50)

      )
    )

    val sortRDD = rdd.sortBy(user => (user.name, user.age), false)

    println(sortRDD.collect().mkString(","))


    sc.stop
  }

  case class User( name: String, age: Int )






}
