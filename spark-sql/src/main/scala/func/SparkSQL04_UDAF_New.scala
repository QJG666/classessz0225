package func

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 通过自定义UDAF(新版弱类型)方式实现求平均年龄
  * 一般用于DSL风格
  */
object SparkSQL04_UDAF_New {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._

    // 创建DF
    val df = spark.read.json("input/user.json")

    // 创建临时视图
    df.createOrReplaceTempView("people")


    // TODO : 自定义UDAF函数
    // TODO : 1.1 创建自定义udaf函数对象
    val myAvg = new MyAvgUDAF2

    // TODO : 1.2 注册函数对象
    //spark.udf.register("myAvg", myAvg)
//    functions.udaf(myAvg)
//    spark.udf.register("myAvg", functions.udaf(myAvg))

    // TODO : 1.3 使用函数进行查询
//    spark.sql("select myAvg(age) from people").show()


    spark.stop()

  }

}

// 输入数据类型
// case class User01(username: String, age: Long)

// 缓存类型
//case class AgeBuffer(var sum: Long, var count: Long)

// 定义一个类，继承Aggregator   select myavg(age) from people
class MyAvgUDAF2 extends Aggregator[Int, AgeBuffer, Double]{
  // 设置初始状态
  override def zero: AgeBuffer = AgeBuffer(0L, 0L)

  // 累加
  override def reduce(buffer: AgeBuffer, age: Int): AgeBuffer = {
    buffer.sum += age
    buffer.count += 1
    buffer
  }

  // 合并
  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 求平均值
  override def finish(buf: AgeBuffer): Double = {
    buf.sum.toDouble / buf.count
  }

  // 指定编解码的方式
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}


