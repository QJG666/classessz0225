package oper

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 连接外部Hive测试
  */
object SparkSQL03_Hive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show tables").show()




    import spark.implicits._




    spark.stop()
  }

}

