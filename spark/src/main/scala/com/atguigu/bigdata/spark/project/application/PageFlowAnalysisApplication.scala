package com.atguigu.bigdata.spark.project.application

import com.atguigu.bigdata.spark.project.common.TApplication
import com.atguigu.bigdata.spark.project.controller.{HotCategoryAnalysisTop10Controller, PageFlowAnalysisController}

/**
  * 页面单跳转换率
  */
object PageFlowAnalysisApplication extends App with TApplication{
  start( appName = "PageFlowAnalysis" ) {
    val controller = new PageFlowAnalysisController
    controller.execute()
  }
}
