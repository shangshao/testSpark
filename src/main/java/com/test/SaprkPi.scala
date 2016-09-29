package com.test

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/22.
 */
object SaprkPi {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2")
    val conf = new SparkConf().setAppName("hh").setMaster("local")
    val sc = new SparkContext(conf)
    val slinces= 100
    val n = 10000000 *slinces
    val count=sc.parallelize(1 to n,slinces).map(i=>{
      var random = Math.random()
      val x = random*2-1
      val y = random*2-1
//      println(x+"----"+y)
      if(x*x+y*y<1) 1 else 0
    }).reduce(_+_)
    println("圆周率是:"+ 4.0 * count/n)
  }
}
