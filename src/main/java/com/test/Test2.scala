package com.test


import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/9.
 */
object Test2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TfIDf").setMaster("local")
    val sc = new SparkContext(conf)
    //Load docuemnts (one pre line)
    val document=sc.textFile("文件").map(_.split(" ").toSeq)
    val hashingTF=new HashingTF()
    val tf = hashingTF.transform(document)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf=idf.transform(tf)
    //首先明确的是它要求的数据源为一篇文章一行


  }
}
