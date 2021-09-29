package com.atguigu.bigdata.spark.project.dao

import com.atguigu.bigdata.spark.project.common.TDao
import com.atguigu.bigdata.spark.project.util.ProjectUtil
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

  def readData() : RDD[String] = {

    ProjectUtil.sparkContext().makeRDD(List("Hello", "Scala"))
  }

}
