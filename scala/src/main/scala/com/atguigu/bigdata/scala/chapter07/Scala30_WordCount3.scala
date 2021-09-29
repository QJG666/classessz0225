package com.atguigu.bigdata.scala.chapter07

import scala.collection.immutable.StringOps
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Scala30_WordCount3 {
  def main(args: Array[String]): Unit = {

    // TODO - Scala - WordCount - Top3
    val dataList = List(
      ("Hello Scala", 4), ("Hello Spark", 2)
    )
    // List (
    // "Hello Scala Hello Scala Hello Scala Hello Scala"
    // "Hello Spark Hello Spark"
    // )
    val strings: List[String] = dataList.map(
      t => {
        val line = t._1
        val count = t._2
        (line + " ") * count
      }
    )
    println(strings)


  }

}
