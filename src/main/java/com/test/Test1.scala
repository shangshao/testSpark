package com.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/9.
 */
object Test1 {
    def main(args:Array[String]): Unit ={
      val conf:Configuration  =new Configuration()
      val fs:FileSystem  = FileSystem.get(conf)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      //设置环境
      var masterUrl = "local[2]"
      val sparkConconf=new SparkConf().setMaster(masterUrl).setAppName("KMeanSemple")
      val sparkContext= new SparkContext(sparkConconf)
      //装载数据
      val fileData =sparkContext.textFile("/user/shangyongqiang/tmp0/hh.txt")
      val parseData=fileData.map(record=>Vectors.dense(record.split(" ").map(_.toDouble)))
      //模型训练
      val dataModeNumber=3
      val dataModelTrainTimes=100
      val model=KMeans.train(parseData,dataModeNumber,dataModelTrainTimes)
      //数据模型的中心点
      println("Cluster centers:")
      for(c<-model.clusterCenters){
        println(" "+c.toString )
      }
      //使用模型测试单点数据
      println("Vectors 0.2 0.2 0.2 is belongs to clusters:"+model.predict(Vectors.dense("0.2 0.2 0.2 ".split(" ").map(_.toDouble))))
      println("Vectors 0.25 0.25 0.25 is belongs to clusters:"+ model.predict(Vectors.dense("0.25 0.25 0.25".split(" ").map(_.toDouble))))
      println("Vectors 8 8 8 is belongs to clusters:" +model.predict(Vectors.dense("8 8 8".split(" ").map(_.toDouble))))
      //交叉评估2,返回数据集和结果
      val path:Path= new Path("/user/shangyongqiang/test/result2")
      if(fs.exists(path)){
        fs.delete(path,true)
      }
      val result2=fileData.map{
        line=>
          val linevectore= Vectors.dense(line.split(" ").map(_.toDouble))
          val prediction= model.predict(linevectore)
          line +" "+ prediction
      }.saveAsTextFile("/user/shangyongqiang/test/result2")
      sparkContext.stop()

    }


}
