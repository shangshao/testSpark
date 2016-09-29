package com.test

import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/25.
 */
object
Test8 {
  def main(args: Array[String]) {
    //加入本地测试的hadoop路径,防止报错
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    val conf = new SparkConf().setMaster("local").setAppName("zhao tongyi ti")
    val sc = new SparkContext(conf)
    //数据是一行
    val input = sc.textFile("file:\\D:\\text8").map(line=>line.split(" ").toSeq)
    val word2Vec = new Word2Vec()
    val model = word2Vec.fit(input)
    //找出同义词
    val synonms = model.findSynonyms("china",40)
    for((synonym,cosineSimilarity)<-synonms){
      println(s"$synonym$cosineSimilarity")
    }
    //saveand load model
    model.save(sc,"myModelPath")
    val sameModel=Word2VecModel.load(sc,"myModelPath")

  }
}
