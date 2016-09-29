package com.sexAge

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by shangyongqiang on 2016/6/22.
 * 用贝叶斯分类来处理年龄分类的问题
 * app的列表是一个个关键词,已知的app专属类别是已经分好的类
 * 用户一个月的使用的app列表以及权重
 * 0030A0F8AD8A64CDEB6AB3CDE6507DC100130088:67|00210160:4|00220166:1
 *
 * Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
 */
object Step2 {
    def main(args: Array[String]) {
val conf = new SparkConf().setAppName("NaiveBayes").setMaster("spark://dmp04:7077")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val srcRDD= sc.textFile("hdfs://10.0.3.171:8020/user/hive/warehouse/syq.db/user_applist/").map( x=>{
          val data =x.split("\u0001")
           val userid=data(0)
           val libsvm: Array[(Int, Double)] =data(1).split("\\|").map(_.split("\\:")).map(x=>(x(0).substring(4).toInt,x(1).toDouble))
           val dis=convsert(libsvm)
            //libsvm可能会有重复去掉重复的数据
              //转换为稀疏向量
              val a1 =0
              val b1 =1
              var aa=0
              val c=libsvm.map(x=>{
                    val a =x._1
                    if (a==0133)  aa=1 else aa
              })
              val a2 =Vectors.sparse(280,dis)
              //稀疏向量转换为稠密向量
             LabeledPoint(aa.toDouble, Vectors.dense(a2.toArray))
      })
          val splits = srcRDD.randomSplit(Array(0.7,0.3))
      //这里的输出目录不能存在否则报错
        splits(1).saveAsTextFile("/user/shangyongqiang/sexAge/test")
          //训练模型
          val  model = NaiveBayes.train(splits(0),lambda = 1.0,modelType = "multinomial")

          //对测试数据集使用训练模型进行分类预测
          val  testpredictionAndLabel = splits(1).map(x=>(model.predict(x.features),x.label))
          //分析统计分类的准确率
          val testccuacy = 1.0*testpredictionAndLabel.filter(x=>x._1==x._2).count()/testpredictionAndLabel.count()
         println(testccuacy)
    }

  def convsert (libsvm: Array[(Int, Double)]): Seq[(Int, Double)] ={
    //将libsvm中的维度去重(可能有重复的会spark运行的时候回报错)
    libsvm.toMap.toSeq
  }

}




















































