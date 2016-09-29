package com.race2

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/4/26.
 */
object Test1 {
  def main(args: Array[String]) {
    val conf =new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sparkSql = new SQLContext(sc)
    import sparkSql.implicits._
    //加载数据
    val srcRDD = sc.textFile("hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/out3/").map {
      x =>
        var a = x.split("\\t")
        var b = a(1).split(",").map(_.toDouble)
        //        Recorde(a(0),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9),a(10))
        LabeledPoint(a(0).toDouble, Vectors.dense(b))
    }
    val splits = srcRDD.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    if (!training.isEmpty()) {

      // Run training algorithm to build the model
      val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(srcRDD)

      // Compute raw scores on the test set.
      val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      //
      //      predictionAndLabels.foreach(x => println(x))

      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      println("Precision = " + precision)

      if (precision > 0.6) {
        val path = "hdfs://192.168.3.151:9000/user/shangyongqiang/bisai/model/model1"
        model.save(sc, path)
        println("saved model to " + path)
      }
    }
  }
}