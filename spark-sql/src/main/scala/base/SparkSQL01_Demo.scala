package base

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {

  def main(args: Array[String]): Unit = {
    // TODO 1.1 创建SparkConf配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLDemo")

    // TODO 1.2 创建SparkSQL程序执行的入口 SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // TODO 注意：spark不是包名，是SparkSession的别名
    import spark.implicits._

    // TODO 2.1 通过读取json文件，创建DF
    val df: DataFrame = spark.read.json("input/user.json")

    //df.show()
    // TODO 2.2 通过SQL风格语法操作DF
    // 创建临时视图
    // df.createOrReplaceTempView("people")
    // 编写SQL对临时视图进行查询
    // spark.sql("select * from people where userage > 30").show()

    // TODO 2.3 通过DSL风格语法操作DF
    // df.select("*").show()
    // df.select($"username", $"userage" + 1)
    // df.select(df("userage") + 1).show()
    // df.select(df.col("username"), df.col("userage") + 1).show()

    // rdd => df => ds
    val rdd = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30)))

    // 将rdd转换为df
    // 如果不指定列名，没办法映射为DS
    //val df1 = rdd.toDF()
    val df1 = rdd.toDF("name", "age")

    // 如果指定的列名和样例类属性名不一致，没办法映射为DS。
    // 映射样例类的时候，是按照列的名字映射的
    // val df1 = rdd.toDF("username", "userage")

    // 将df转换为ds
    val ds = df1.as[User01]
    // ds.show()

    // ds => df => rdd
    val df2 = ds.toDF()
    val rdd1 = df2.rdd
//    rdd1.map(
//      row => {
//        (row.getString(0), row.getInt(1))
//      }
//    ).foreach(println)

    //rdd1.foreach(println)

    // DS => RDD
    val rdd3 = ds.rdd

    // RDD => DS
    val ds2 = rdd3.toDS()
    ds2.show()

    spark.stop()

  }

  case class User01(name: String, age: Int)
  //case class User01(username: String, userage: Int)

}
