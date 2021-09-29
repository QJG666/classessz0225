package com.atguigu.bigdata.spark.project.common

import com.atguigu.bigdata.spark.project.util.ProjectUtil

trait TDao {

  def readFile( path: String ) = {
    ProjectUtil.sparkContext().textFile(path)
  }

}
