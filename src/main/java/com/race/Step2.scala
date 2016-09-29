package com.race

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by shangyongqiang on 2016/4/14.
 * "月份","IMSI","网别","性别","年龄值段","ARPU值段","终端品牌"
 * ,"终端型号","流量使用量","语音通话时长","短信条数"
 *
 *time                	string
  imsi                	string
  net                 	string
  sex                 	string
  age                 	string
  arpu                	string
  mtype               	string
  model               	string
  data                	string
  phone               	string
  sms                 	string
 *
 * "201501","d8ccc2441daabc76628b8ce9ffc9446e","3G","男","30-39","50-99"
 * ,"Xiaomi","MI 2013029","0-499","377","0"
 */
object Step2 {

  case class Recorde(time: String, imsi: String, net: String, sex: String, age: String, arpu: String, mtype: String, model: String, data: String, phone: String, sms: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Step1").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sparkSql = new SQLContext(sc)
    //加载数据
    val srcRDD = sc.textFile("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/predict/").map {
      x =>

        var a = x.split("\\t")
        var b = a(1).split(",").map(_.toDouble)
        //        Recorde(a(0),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),a(10))
        (a(0), Vectors.dense(b))
    }
//    val splits = srcRDD.randomSplit(Array(0.8, 0.2), seed = 11L)
//    val training = splits(0)
//    val test = splits(1)

    if (!srcRDD.isEmpty()) {

      // Run training algorithm to build the model
//      val model = new LogisticRegressionWithLBFGS()
//        .setNumClasses(11)
//        .run(training)
      val model = LogisticRegressionModel.load(sc, "hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/model/model1")
      // Compute raw scores on the test set.
      val predictionAndLabels = srcRDD.map { features =>
        val prediction = model.predict(features._2)
        (features._1,prediction)
      }
//
//      predictionAndLabels.foreach(x => println(x))
      predictionAndLabels.saveAsTextFile("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/result/")

      // Get evaluation metrics.
//      val metrics = new MulticlassMetrics(predictionAndLabels)
//      val precision = metrics.precision
//      println("Precision = " + precision)

//      if (precision > 0.6) {
//        val path = "hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/model/model1"
//        model.save(sc, path)
//        println("saved model to " + path)
//      }
    }
  }
}