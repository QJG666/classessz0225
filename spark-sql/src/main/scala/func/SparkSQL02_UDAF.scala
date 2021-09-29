package func

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 通过自定义UDAF(弱类型)方式实现求平均年龄
  * 一般用于SQL风格
  */
object SparkSQL02_UDAF {

  def main(args: Array[String]): Unit = {

    // 创建Spark配置文件对象
    // val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL_UDF")
    // 创建SparkSession对象
    // val spark = SparkSession.builder().config(conf).getOrCreate()

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._

    // 创建DF
    val df = spark.read.json("input/user.json")

    // 创建视图
    df.createOrReplaceTempView("people")

    // TODO : 自定义UDAF函数
    // TODO : 1.1 自定义udaf函数对象
    val myAvg = new MyAvgUDAF

    // TODO : 1.2 注册函数对象
    spark.udf.register("myAvg", myAvg)

    // TODO : 1.3 使用函数
    spark.sql("select myAvg(userage) from people").show


    spark.stop()

  }

}

// select myavg(age) from user
class MyAvgUDAF extends UserDefinedAggregateFunction{
  // 输入数据类型
  override def inputSchema: StructType = StructType(List(StructField("age", IntegerType)))

  // 缓存数据类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("ageSum", LongType), StructField("count", LongType)))
  }

  // 输出数据类型
  override def dataType: DataType = DoubleType

  // 精准性校验
  override def deterministic: Boolean = true

  // 设置缓存初始状态
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 更新缓存状态
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val age = input.getInt(0)
    buffer(0) = buffer.getLong(0) + age
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 合并缓存数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 返回输出结果
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
