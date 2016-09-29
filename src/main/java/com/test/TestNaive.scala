package com.test

/**
 * Created by shangyongqiang on 2016/4/11.
 *  package com.test
*/
  import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
  import org.apache.spark.mllib.classification.NaiveBayes
  import org.apache.spark.mllib.linalg.{Vector, Vectors}
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.sql.Row
  import org.apache.spark.{SparkConf, SparkContext}

object TestNaive {

  /**
   * Created by shangyongqiang on 2016/4/8.
   */

    case class RawDataRecord(category:String,text:String)
    def main(args: Array[String]) {
      print("********************")
      System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
      val conf = new SparkConf().setAppName("hhhhhhh").setMaster("local[4]")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      var srcRDD = sc.textFile("hdfs://192.168.3.151:9000/user/shangyongqiang/test/hh/").map{
        x=>
          var data =x.split(",")
          RawDataRecord(data(0),data(1))
      }
      //70%作为训练诗句.30%作为测试数据
      val splits = srcRDD.randomSplit(Array(0.7,0.3))
      var trainingDF = splits(0).toDF()
      val testDF = splits(1).toDF()

      //将词语转换成数组
      var  tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
      var wordsData = tokenizer.transform(trainingDF)
      println("output1")
      wordsData.select($"category",$"words",$"text").take(1)



      var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeature")
      var featureData = hashingTF.transform(wordsData)
      println("output2:")
      featureData.select($"category",$"words",$"rawFeature").take(1)

      var idf = new IDF().setInputCol("rawFeature").setOutputCol("feature")
      var idfModel = idf.fit(featureData)
      var rescalData = idfModel.transform(featureData)
      println("output3:")
      rescalData.select($"category",$"feature").take(1)
      //转成bayes的输入格式
      val trainData = rescalData.select($"category", $"feature").map {
        case Row(label: String,features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }
      println("output4:")
      trainData.take(1)
      //训练模型
      val model = NaiveBayes.train(trainData,lambda = 1.0,modelType ="multinomial" )
      //测试数据集,做同样的特征表示及格式转换
      var testwordsData = tokenizer.transform(testDF)
      var testfeatureData = hashingTF.transform(testwordsData)
      var testrescaledData = idfModel.transform(testfeatureData)
      val testDataRDD = testrescaledData.select($"category",$"feature").map{
        case Row(label: String, features: Vector) =>
          LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      }

      //对测试数据集使用训练模型进行分类预测
      val  testpredictionAndLabel=testDataRDD.map(p=>(model.predict(p.features),p.label))
      //统计分类的准确率
      var testccuacy =1.0*testpredictionAndLabel.filter(x=>x._1==x._2).count()/testpredictionAndLabel.count()
      println("output5:")
      println(testccuacy)
    }

}
