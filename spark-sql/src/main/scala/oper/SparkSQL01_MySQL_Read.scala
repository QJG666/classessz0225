package oper

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 从MySQL数据源读取数据创建DF
  */
object SparkSQL01_MySQL_Read {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._

    // TODO : 从MySQL数据源读取数据创建DF
    // 方式1
//    var prop = new Properties()
//    prop.setProperty("user", "root")
//    prop.setProperty("password", "123456")
//    val df = spark.read.jdbc("jdbc:mysql://hadoop106:3306/test", "user", prop)

    // 方式2
//    spark.read.format("jdbc")
//        .option("url", "jdbc:mysql://hadoop106:3306/test?user=root&password=123456")
//        .option("driver", "com.mysql.jdbc.Driver")
//        .option("user", "root")
//        .option("password", "123456")
//        .option("dbtable", "user")
//        .load().show()

    // 方式3
    spark.read.format("jdbc")
        .options(
          Map(
            "url"->"jdbc:mysql://jdbc:mysql://hadoop106:3306/test?user=root&password=123456",
            "dbtable"->"user",
            "driver"->"com.mysql.jdbc.Driver"
          )
        ).load().show


    spark.stop()
  }

}
