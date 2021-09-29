package com.atguigu.bigdata.scala.chapter06

object Scala03_Object_Import {

    def main(args: Array[String]): Unit = {

      // TODO Scala 面向对象编程 - import
      // java import的作用
      // 1. 导入指定包中的类
      // 2. 静态导入
      // java中的import关键字功能比较单一，所以scala进行扩展。
      // 1. import确实可以导包
      //      import java.util
      // new util.ArrayList()
      // 2. import可以导入包中所有的类
      //      import java.util._
      //      即使是用下划线表示导入这个包中所有的类，但是在编译时，会自动查找使用的类进行导入
      //      而不是全部导入
      // new ArrayList()
      // 3. import关键字可以声明在任意的地方
      //    import java.util.Date
      // println(new Date())

      // 4. scala中有3个可以默认导入的内容
      //    4.1 java.lang包
      //    4.2 scala包
      //    4.3 Predef对象
      // println("import...")

      // 5. 如果导入一个包中的多个类，但是又不希望使用下划线或多次import声明
      //      可以使用特殊语法将一个包中的多个类声明在一行中
      //      import java.util.{ArrayList, HashMap, LinkedList}
//      new ArrayList()
//      new HashMap()
//      new LinkedList()

      // 6. 隐藏类，将指定的类隐藏，不进行导入
      //      import java.sql.{Date=>_, Array=>_, _}
//      new Date()
//      new Timestamp()

      // 7. 支持类的重命名
      // import java.util.{Date=>UtilDate, _}
      // println(new UtilDate())

      // 8. Scala中默认的导入包路径其实是相对路径，从当前包下开始查找
      //     如果不想使用相对路径，而是绝对路径，需要采用特殊方式
      //     使用_root_
      println(new _root_.java.util.HashMap)

    }

  }
//package java {
//  package util {
//
//    class HashMap {
//
//    }
//  }
//}
