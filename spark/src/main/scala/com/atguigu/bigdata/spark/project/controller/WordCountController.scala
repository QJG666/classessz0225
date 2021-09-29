package com.atguigu.bigdata.spark.project.controller

import com.atguigu.bigdata.spark.project.common.TController
import com.atguigu.bigdata.spark.project.service.WordCountService

class WordCountController extends TController{

  private val wordCountService = new WordCountService


  /**
    * 执行控制器
    * @return
    */
  def execute() = {

    // 分析数据
    val result = wordCountService.analysis()

    // 打印结果
    result.collect().foreach(println)
  }


}
