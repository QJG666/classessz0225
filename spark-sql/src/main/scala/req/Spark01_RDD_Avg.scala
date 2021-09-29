package req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Avg {

  def main(args: Array[String]): Unit = {

    // Spark 程序分为几步?
    // TODO 1. 获取spark的连接对象(上下文环境对象)
    // 创建Spark的配置对象
    // local表示本地环境，如果后面增加数字，表示执行程序时使用的本地线程的数量(虚拟CPU核)
    // 如果使用local[*]表示使用当前节点中最大的CPU核
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val res = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40))).map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    println(res._1 / res._2)



    sc.stop()

  }

}
