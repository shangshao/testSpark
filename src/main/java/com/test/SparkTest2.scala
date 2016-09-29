package com.test

import java.util

import org.apache.spark.ml.feature.{IDF, Tokenizer, HashingTF}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/4/6.
 */
object SparkTest2 {
  case class RwaDataRecord(category:String,text:String)
  def main(args: Array[String]) {
    val conf= new SparkConf().setMaster("yarn-clint")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //将原始数据映射到DataFrame中,字段category为分类编号,字段text为分好的词,以空格分隔
    var srcDF = sc.textFile("数据路径").map{
      x=>
        var data = x.split(",")
        RwaDataRecord(data(0),data(1))
    }.toDF()

    srcDF.select("catetory,text").take(2).foreach(println)
    //将分好的词转换为数组
    var tokenier = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsDatas = tokenier.transform(srcDF)
    wordsDatas.select($"catetory",$"text",$"words").take(2).foreach(println)

    //将每个词转成int行,并计算其在文档中的词频(TF)
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeature").setNumFeatures(100)
    var featurizedData = hashingTF.transform(wordsDatas)
    featurizedData.select($"category",$"words",$"rawFeature").take(2).foreach(println)
    //计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeature").setOutputCol("feature")
    var idfmodel =idf.fit(featurizedData)
    var rescalaedData = idfmodel.transform(featurizedData)
    rescalaedData.select($"category",$"words",$"features").take(2).foreach(println)

    /*

    需要经数据转换成下面的这种格式以便于处理
   [0,WrappedArray(关键词1, 关键词2, 关键词1, 关键词3),(100,[23,81,96],[0.0,0.4054651081081644,0.4054651081081644])]
   [1,WrappedArray(关键词1, 关键词4 , 关键词5),(100,[23,72,92],[0.0,0.4054651081081644,0.4054651081081644])]
              0,1 0 0
              0,2 0 0
              0,3 0 0
              0,4 0 0
              1,0 1 0
              1,0 2 0
              1,0 3 0
              1,0 4 0
              2,0 0 1
              2,0 0 2
              2,0 0 3
              2,0 0 4
     */
//    var triainDataRdd = rescalaedData.select($"catetory",$"features").map{
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray[Double]))
//    }


  }
}
