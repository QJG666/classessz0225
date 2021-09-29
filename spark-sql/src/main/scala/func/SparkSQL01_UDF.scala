package func

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL01_UDF {

  def main(args: Array[String]): Unit = {

    // 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL_UDF")
    // 创建SparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // TODO : 自定义UDF函数
    // TODO 1.1 注册一个函数
    spark.udf.register("sayHi", (name: String) => {"hello-->" + name})

    // TODO 1.2 创建DF
    val df = spark.read.json("input/user.json")

    // TODO 1.3 创建临时视图
    df.createOrReplaceTempView("people")

    // TODO 1.4 编写sql，查询临时视图
    spark.sql("select sayHi(username) from people").show



    spark.stop()

  }

}
