package com.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/15.
 */
object Test3 {
  def main(args: Array[String]) {
    var a = 0
    for(i<-1 to 10){
      //范围是1到10
      println("程序运行第"+i+"次"+"a和i加起来是"+(i+a))
      a+=1
    }
    val numCluster=8 //聚类的个数
    val numIterations=20 //迭代的次数
    val parallRunNums=5 //并行度
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("shangTest")
    val sparkContext = new SparkContext(sparkConf)
    val data =sparkContext.textFile("要处理的目标文件")
    val parseData = data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble)))
    val clusters = KMeans.train(parseData,numCluster,numIterations,parallRunNums)
    //即可得到聚类结果
    //聚类中心
    val clusterCenter = clusters.clusterCenters
    //聚类结果标签
    val labels = clusters.predict(parseData)
    //保存结果
    labels.saveAsTextFile("保存路径")
  }
}
