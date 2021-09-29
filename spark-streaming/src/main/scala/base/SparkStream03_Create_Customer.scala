package base

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 自定义数据源，实现监控某个端口号，获取该端口号内容
  */
object SparkStream03_Create_Customer {

  def main(args: Array[String]): Unit = {

    // 1. 创建Spark配置文件对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkDemo")

    // 2. 创建Streaming程序的执行入口
    val ssc = new StreamingContext(conf, Seconds(3))

    // 3. 自定义数据源  创建DStream
    //ssc.socketTextStream()
    val lineDS = ssc.receiverStream(new MyReceiver( "hadoop106", port = 9999 ))

    lineDS
      .flatMap(_.split(" "))
      .map(( _, 1 ))
      .reduceByKey(_+_)
      .print()

    // 开始采集
    ssc.start()

    ssc.awaitTermination()
  }

}

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  private var socket: Socket = _

  override def onStart(): Unit = {
    //logInfo(s"Connecting to $host:$port")

    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
    //logInfo(s"Connected to $host:$port")

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run(): Unit = { receive() }
    }.start()

  }

  override def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
        // logInfo(s"Closed socket to $host:$port")
      }
    }
  }

  def receive() = {
    // 读取指定端口的数据
    val bf = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    // 定义一个变量，存储读取到的一行数据
    var line: String = null

    while ( (line = bf.readLine()) != null ) {
      // 调用store方法，将读到的数据进行缓存
      store(line)
    }


  }
}
