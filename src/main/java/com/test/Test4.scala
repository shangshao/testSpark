package com.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/15.
 */
object Test4 {
  //saprk sql
  //spark streaming
  //mllib
  def main(args: Array[String]) {
    val datapath = "/home/shangyongqiang/test.txt"
    val conf  = new SparkConf().setMaster("local[3]").setAppName("矩阵数据聚类")
    val sparkcontext = new SparkContext(conf)
    val data = sparkcontext.textFile(datapath)
    val examples= data.map{
          //空间上的一个点
      line=>Vectors.dense(line.split(" ").map(_.toDouble))
    }.cache()
    val numExamples = examples.count()
    println("----------华丽的分割线----------")
    //建立模型
    val maxIterations= 20
    val k = 2
    val runs =2
    val initalizationMode= "k-means||"
    val model=KMeans.train(examples,k,maxIterations,runs,initalizationMode)
    //计算测试误差
    val cost = model.computeCost(examples)
    println("Total cost = $cost.")
  }

}
