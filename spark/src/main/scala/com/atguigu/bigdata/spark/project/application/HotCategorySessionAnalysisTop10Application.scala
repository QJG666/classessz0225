package com.atguigu.bigdata.spark.project.application

import com.atguigu.bigdata.spark.project.common.TApplication
import com.atguigu.bigdata.spark.project.controller.{HotCategoryAnalysisTop10Controller, HotCategorySessionAnalysisTop10Controller}

/**
  * 热门品类Top10 活跃Session统计 应用
  */
object HotCategorySessionAnalysisTop10Application extends App with TApplication{
  start( appName = "HotCategorySessionAnalysisTop10" ) {
    val controller = new HotCategorySessionAnalysisTop10Controller
    controller.execute()
  }
}
