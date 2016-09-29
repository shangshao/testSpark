package com.test

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shangyongqiang on 2016/3/29.
 */
class Itemers {
  def main(args: Array[String]) {
    //协同过滤要求数据是用户,商品,评分
    val conf = new SparkConf().setAppName("xietong guolv").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(".....").map(_.split(",")match {
      case Array(user,item,rate) =>Rating(user.toInt,item.toInt,rate.toDouble)
    })
    //Build tthe redcommmemendation model using ALs
    val numIterations = 20
    val model =ALS.train(data,1,20,0.01)

    //Evalete the model on rating data399
    val userProducts=data.map{
      case Rating(user,item,rate)=>(user,item)
    }
    val predictions = model.predict(userProducts).map{
      case Rating(user,item,rate)=>((user,item),rate)
    }
    val ratesAndPreds = data.map{
      case Rating(user,item,rate)=>((user,item),rate)
    }.join(predictions)
    val MSE=ratesAndPreds.map{
      case((user,item),(r1,r2))=>math.pow((r1-r2),2)
    }.reduce(_+_)/ratesAndPreds.count

    print("Mean Squared Error ="+ MSE)


  }
}
