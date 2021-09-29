package req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过累加器实现求平均年龄
  */
object Spark02_Acc_Avg {

  def main(args: Array[String]): Unit = {

    // TODO 1. 获取spark的连接对象(上下文环境对象)
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    // 创建累加器对象
    val myac = new MyAccumulator

    // 注册累加器
    sc.register(myac)

    // 使用累加器
    rdd.foreach{
      case (name, age) => {
        myac.add(age)
      }
    }

    println(myac.value)

    sc.stop()

  }

}

class MyAccumulator extends AccumulatorV2[Int, Double] {

  private var sum: Int = 0
  private var count: Int = 0

  override def isZero: Boolean = {
    sum == 0 && count == 0
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    val myac = new MyAccumulator
    myac.sum = this.sum
    myac.count = this.count
    myac
  }

  override def reset(): Unit = {
    sum = 0
    count = 0
  }

  override def add(age: Int): Unit = {
    sum += age
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {

//    val oo = other.asInstanceOf[MyAccumulator]
//    sum += oo.sum
//    count += oo.count

    other match {
      case myac: MyAccumulator => {
        sum += myac.sum
        count += myac.count
      }
      case _ =>
    }
  }

  override def value: Double = sum.toDouble / count
}

