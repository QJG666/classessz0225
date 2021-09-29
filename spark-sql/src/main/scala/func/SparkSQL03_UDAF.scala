package func

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 通过自定义UDAF(强类型)方式实现求平均年龄
  * 一般用于DSL风格
  */
object SparkSQL03_UDAF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._

    // 创建DF
    val df = spark.read.json("input/user.json")

    //
    val ds = df.as[User01]

    // TODO : 自定义UDAF函数
    // TODO : 1.1 创建自定义udaf函数对象
    val myAvg = new MyAvgUDAF1

    val column = myAvg.toColumn

    // TODO : 1.2 使用自定义函数
    ds.select(column).show()


    spark.stop()

  }

}

// 输入数据类型
case class User01(username: String, age: Long)

// 缓存类型
case class AgeBuffer(var sum: Long, var count: Long)

class MyAvgUDAF1 extends Aggregator[User01, AgeBuffer, Double]{
  // 设置初始值
  override def zero: AgeBuffer = AgeBuffer(0L, 0L)

  //  对传入的值进行聚合操作
  override def reduce(buffer: AgeBuffer, user: User01): AgeBuffer = {
    // 获取用户的年龄
    val age = user.age
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

  // 返回结果
  override def finish(buffer: AgeBuffer): Double = {
    buffer.sum.toDouble / buffer.count
  }


  // 指定编解码方式  固定的写法
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}


