package com.atguigu.bigdata.spark.project.service

import com.atguigu.bigdata.spark.project.common.TService
import com.atguigu.bigdata.spark.project.dao.WordCountDao

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao

  /**
    * 数据分析
    * @return
    */
  override def analysis() = {

    // 操作数据
    val dataRDD = wordCountDao.readFile("input/1.txt")

    val wordRDD = dataRDD.flatMap(
      line => {
        line.split(" ")
      }
    )

    val word2OneRDD = wordRDD.map((_, 1))
    val word2CountRDD = word2OneRDD.reduceByKey(_ + _)
    word2CountRDD
  }


}
