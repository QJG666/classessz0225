package com.atguigu.bigdata.scala.chapter05

object Scala10_Function9 {

  def main(args: Array[String]): Unit = {

    // test(10)(20)(_+_) => 30
    def test(i: Int)(j: Int)(f:(Int, Int) => Int): Int = {
      f( i, j )
    }

    println(test(10)(20)(_ + _))
    // 函数柯里化：将复杂的函数参数列表变得简单化。
    //   将多个参数的参数列表变成多个参数列表
    //   我们将这个转换的操作称之为函数柯里化
    // val xxx = 1 + 2 + 3 + 4 .. + 99 => 1min
    // val yyy = 1 + ... + xxx  => 5min
    // val zzz = 1... + yyy  => 10min
    // def test(xxx)(yyy)(zzz) = {

    // }

    // def test( xxx, yyy, zzz ) = {

      // var sum = 0
//      for ( i <- 1 to xxx ) {
//        sum += i
//      }
      // 1min

//      for ( i <- 1 to sum ) {
//        sum += i
//      }
      // 5min

//      for ( i <- 1 to sum ) {
//        sum += i
//      }
      // 10min

    // }

  }

}
