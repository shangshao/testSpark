package com.job1

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by shangyongqiang on 2016/8/31.
 */
object SparkLda {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testLda").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //输入的文件是每行用词频向量表示一篇文档
    val data =sc.textFile("")
    val parsedData = data.map(s=>Vectors.dense(s.trim.split(" ").map(_.toDouble)))
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()
    val ladModel = new LDA().setK(3).run(corpus)
    //打印主题
    println("Learn topic(as distributions over vocab of "+ladModel.vocabSize +" words:)")
    val toptics = ladModel.topicsMatrix
    for(toptic<-Range(0,3)){
      print("Topic"+toptic+":")
      for(word<-Range(0,ladModel.vocabSize)){
        print(" "+toptics(word,toptic))
      }
    }
  }
}
