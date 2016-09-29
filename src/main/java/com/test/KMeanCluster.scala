package com.test

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by shangyongqiang on 2016/3/10.
 */

//这个是的数据类型是以下这样的一共是9维
/**
 *
Channel	Region	Fresh	Milk	Grocery	Frozen	Detergents_Paper	Delicassen
2	3	12669	9656	7561	214	2674	1338
2	3	7057	9810	9568	1762	3293	1776
2	3	6353	8808	7684	2405	3516	7844
1	3	13265	1196	4221	6404	507	1788
2	3	22615	5410	7198	3915	1777	5185
2	3	9413	8259	5126	666	1795	1451
2	3	12126	3199	6975	480	3140	545
2	3	7579	4956	9426	1669	3321	2566
1	3	5963	3648	6192	425	1716	750
 */
object KMeanCluster {

  def isColumnNameLine(line: String) :Boolean = {
    if(line!=null && line.contains("Channel"))
      true else false
  }

  def main(args: Array[String]) {
    if(args.length<5){
      println("Useage:KmeanClustering trainingDataFilePath testData numcluster numIteration runTimes")
      sys.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Mlib Exercise:K-Means Clustering")
    val sc = new SparkContext(conf)
    val rawTrainingData=sc.textFile(args(0))
    val parsedTrainingData = rawTrainingData.filter(!isColumnNameLine(_)).map(line=>{
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()
    //Cluster the data into two classsses using Kmeans
    val numClusters = args(2).toInt
    val numIterations = args(3).toInt
    val runTimes = args(4).toInt
    val cluster:KMeansModel=KMeans.train(parsedTrainingData,numClusters,numIterations,runTimes)
    var clusterIndex:Int = 0
    println("Cluster Number:"+cluster.clusterCenters.length)
    println("Cluster Centers Information Overview:" )
    cluster.clusterCenters.foreach(x=>{
      println("center point of cluster"+ clusterIndex+":")
      println(x)
      clusterIndex=clusterIndex+1
    })
    //begin to check which cluster each test data belongs to based on the clustering result
    val rawTestData = sc.textFile(args(1))
    val parsedTestData = rawTestData.map(line=>{
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(testDataline=>{
      val predictedClusterIndex = cluster.predict(testDataline)
      println("The Data" + testDataline.toString+"belongs to cluster"+predictedClusterIndex)
    })
    println("Spark MLlib K-means clusering test finished.")
//关于k值的选择
    //为什么是3到20
    val ks: Array[Int] = Array(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    ks.foreach(cluster=>{
      val model:KMeansModel=KMeans.train(parsedTrainingData,cluster,30,1)
      val ssd =model.computeCost(parsedTrainingData)
      println("sum of squared distances of points to their nearest center when k="+cluster+"->"+ssd)
    })
  }
}
