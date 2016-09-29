package com.sexAge

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/6/29.
 */
object Test {
  def convsert (libsvm: Array[(Int, Double)]): Seq[(Int, Double)] ={
    libsvm.toSeq
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NaiveBayes").setMaster("spark://dmp04:7077")
    val sc = new SparkContext(conf)

    val lib =Array((1,2.0),(1,2.0),(1,2.0))
    sc.parallelize(lib).reduceByKey(_+_)
    lib.foreach(x=>{
      println(x._1)
    })
    println(convsert(lib).size)
    val b ="fsfdsfdsfdsfdsfdsfdsfds"
    val c: Char ='f'
    println(b.charAt(0).equals(c))
  }

}
