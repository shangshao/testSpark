package com.test

import org.apache.spark.ml.feature.{CountVectorizer, Word2Vec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/4/12.
 */
object TestWord3Vec {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("shangyognqwiang ").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val doucumentDF = sqlContext.createDataFrame(Seq(
    "11 22 33".split(" "),
    "44 55 66".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(1)
    val model = word2Vec.fit(doucumentDF)
    val result = model.transform(doucumentDF)
    result.collect().foreach(println)


    val df = sqlContext.createDataFrame(Seq(
      (0,Array("","","")),(1,Array("","",""))
    )).toDF("id","words")
    var cvModel = new CountVectorizer().setInputCol("words").setOutputCol("feature").setVocabSize(5).setMinDF(1).fit(df)
    println("output2:")
    cvModel.transform(df).select("id","words","feature").collect().foreach(println)

  }
}
