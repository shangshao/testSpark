package com.race2

import java.io.StringReader

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.lucene.IKAnalyzer

/**
 * Created by shangyongqiang on 2016/5/19.
 * 读取hadoop目录下的数据进行分析
 */
object SparkHive {
  case class Recode(id:String,text:String)
  def main(args: Array[String]) {
      val conf  =new SparkConf().setAppName("TestSparkHive")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val srcRDD=sc.textFile("hdfs://mycluster/user/shangyongqiang/sparkin/a.txt").map{
      x=>
       var data = x.split("\u0001")
        Recode(data(0),data(1))
    }
    //将文本进行分词.setInputCol("text").setOutputCol("words")
    /*
      //创建分词对象
        Analyzer anal=new IKAnalyzer(true);
        StringReader reader=new StringReader(text);
        //分词
        TokenStream ts=anal.tokenStream("", reader);
        CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
        //遍历分词数据
        while(ts.incrementToken()){
            System.out.print(term.toString()+"|");
        }
        reader.close();
        System.out.println();
     */
//    var  tokenizer = new IKAnalyzer().tokenStream("",new StringReader(""))
//    val reader = new StringReader()
  }
}

