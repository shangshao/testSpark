package com.test

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/17.
 */
object SparkCluster {
  def main(args: Array[String]) {
    println("这是测试")
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    val conf = new SparkConf().setAppName("test Spark cluster").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("hdfs://192.168.3.151:9000/user/shangyongqiang/tmp0/number.txt")
    val parseData = data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble))).cache()
    val numCluster=6
    val numIteractions = 20
    val clusters = KMeans.train(parseData,numCluster,numIteractions)
    val WSSSE = clusters.computeCost(parseData)
    println("within set num of squared error = "+WSSSE)
    val ss = parseData.map(v=>v.toString()+"belong to cluster"+clusters.predict(v)).collect()
    ss.foreach(a=>println(a.toString))
    val labels = clusters.predict(parseData)
    //保存结果
    labels.saveAsTextFile("hdfs://192.168.3.151:9000/user/shangyongqiang/test/ok.txt")
    var clusterIndex:Int = 0
    println("***********************")
    println("Cluster Number:"+clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:" )
    clusters.clusterCenters.foreach(x=>{
      //类的中心是数据中的点,可以理解是具有代表性的点(如果是)
      println("center point of cluster"+ clusterIndex+":")
      println(x)
      clusterIndex=clusterIndex+1
    })
  }
}
