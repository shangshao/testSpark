package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/28.
 */
object Kmeans {
  def main(args: Array[String]) {
    //屏蔽不必呀的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Kmans").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //装载数据集
    val data = sc.textFile("/home/shangyongqiang/hh/kmeans.txt",1).map(s=>Vectors.dense(s.split(" ").map(_.toDouble)))

    //将数据集聚类,2个类,20次迭代,进行模型训练形成数据模型
    val numClusters=2
    val numIterrations = 20
    val model=KMeans.train(data,numClusters,numIterrations)

    //打数据模型的中心店
    println("Cluster centers:")
    for(c<-model.clusterCenters){
      println(" "+c.toString)
    }

    //使用误差平方之和来评估数据模型
    val cost  = model.computeCost(data)
    println("WithIn set sum of squared error ="+cost)

    //使用模型测试单点数据
    println(model.predict(Vectors.dense("需要预测的点".split(" ").map(_.toDouble))))

    //交叉评估1.只返回结果
    val testdata = model.predict(data)
    testdata.saveAsTextFile("")

    //交叉评估2,返回数据集和结果
    val result =data.map{
      line=>
        val prediction = model.predict(line)
        line +""+prediction
    }.saveAsTextFile("")
    sc.stop()
  }

}
