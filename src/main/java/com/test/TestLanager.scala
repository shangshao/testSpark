package com.test

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Matrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * Created by shangyongqiang on 2016/3/24.
 */
object TestLanager {
  def main(args: Array[String]) {
    //->这个符号的左右两边分别是kv
    val map = Map(1->2,2->3)
    println(map.get(4) getOrElse("不存在")  )
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    val a = Vector(1,1.1,2)
    val b = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    println(b.toJson)
    val pos = LabeledPoint(1,Vectors.dense(1.0, 0.0, 3.0))
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
    println(pos.toString())
    println(neg.label)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    //数据的格式为libsvm
    /*
     实际运用中,稀疏数据是很常见的。MLlib可以读取以LIBSVM格式存储的训练实例,LIBSVM格式是 LIBSVM 和 LIBLINEAR的默认格式,这是一种文本格式,每行代表一个含类标签的稀疏特征向量。格式如下:
         label index1:value1 index2:value2 ...
        索引是从 1 开始并且递增。加载完成后,索引被转换为从 0 开始。
        通过 MLUtils.loadLibSVMFile读取训练实例并以LIBSVM 格式存储。
     */
    val examples :RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc,"data/mllib/sample_libsvm_data.txt")
    //本地矩阵法
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    //三行两列
    /*
    1.0, 2.0
    3.0, 4.0
    5.0, 6.0
     */
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
  }
}
