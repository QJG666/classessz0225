package com.atguigu.bigdata.scala.chapter08

object Scala02_Match1 {
  def main(args: Array[String]): Unit = {

    // Scala - 模式匹配
    val age = 50

    // scala中的模式匹配没有break。使用大括号表示逻辑执行范围
    // scala中执行分支，不需要break语法，两个case之间的代码执行完成后，自动跳出
    // scala执行的时候，会首先判断case分支，然后再判断取值
    // scala执行的时候，如果没有能够匹配成果的分支，那么会出现错误：scala.MatchError
    // match表达式语法存在返回值，返回值为满足条件分支的最后一行代码的结果
    age match {


      case 20 => println("age is 20")

      case 30 => {
        println("age is 30")
      }

//      case _ => {
//        println("age is not 20 or 30")
//      }

    }


  }

}
