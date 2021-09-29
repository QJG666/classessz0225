package com.atguigu.bigdata.spark.project.application

import com.atguigu.bigdata.spark.project.common.TApplication
import com.atguigu.bigdata.spark.project.controller.HotCategoryAnalysisTop10Controller

object HotCategoryAnalysisTop10Application extends App with TApplication{
  start( appName = "HotCategoryAnalysisTop10" ) {
    val controller = new HotCategoryAnalysisTop10Controller
    controller.execute()
  }
}
