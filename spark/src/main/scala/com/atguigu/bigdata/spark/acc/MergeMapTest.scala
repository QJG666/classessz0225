package com.atguigu.bigdata.spark.acc

import scala.collection.mutable

object MergeMapTest {

  def main(args: Array[String]): Unit = {

    val map1 = mutable.Map("zhangsan" -> 31, "lisi" -> 21, "wangwu" -> 41)
    val map2 = mutable.Map("zhangsan" -> 40, "lisi" -> 30, "wangwu" -> 50)

    val mergeMAP = map1.foldLeft(map2)(
      (map, kv) => {
        val k = kv._1
        val v = kv._2
        map(k) = map.getOrElse(k, 0) + v
//        println("map = " + map)
//        println("kv = " + kv)
        map
      }
    )

    println(map1.size)
     // println(mergeMAP)

//    mergeMAP.foreach(
//      s => {
//        println("******************")
//        s
//
//      }
//    )


  }

}
