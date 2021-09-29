package oper

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 将DF数据写入到MySQL数据库
  */
object SparkSQL02_MySQL_Writer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._

    // TODO : 读取文件创建DF
   val df = spark.read.json("input/user.json")
//    df.show()

    // TODO 1.1 通过jdbc方法，保存数据到MySQL
//    val props = new Properties()
//    props.setProperty("user", "root")
//    props.setProperty("password", "123456")
//    df
//      .write
//      .mode(SaveMode.Append)
//      .jdbc("jdbc:mysql://hadoop106:3306/test", "user", props)

    // TODO 1.2 通用的方式 format指定写出类型
    val ds = df.as[People]

    ds.write
        .format("jdbc")
        .option("url", "jdbc:mysql://hadoop106:3306/test")
        .option("user", "root")
        .option("password", "123456")
        .option("dbtable", "user")
        .mode(SaveMode.Append)
        .save()


    spark.stop()
  }

}

// case class People(name: String, age: Int)
case class People(name: String, age: Long)
