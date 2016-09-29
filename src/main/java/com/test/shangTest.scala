package com.test

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.feature.{Word2Vec => OldWord2VecModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by shangyongqiang on 2016/3/8.
 */
object shangTest {
  def main(args: Array[String]) {
    print("********************")
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val documentDF=sqlContext.createDataFrame(Seq("apple guanwang apple xuanbu".split(" "), "apple li bnanna".split(" ")).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(1)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.collect().foreach(println)

  }

}
