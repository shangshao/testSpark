package com.test

import java.io.File
import java.util

import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.{ToAnalysis, BaseAnalysis}
import org.ansj.util.{FilterModifWord, MyStaticValue}
import org.nlpcn.commons.lang.tire.domain.Forest
import org.nlpcn.commons.lang.tire.library.Library

/**
 * Created by shangyongqiang on 2016/3/30.
 */
object TextFenci extends UserDefineLibrary{
//  MyStaticValue.userLibrary="dic/test"
  def main(args: Array[String]) {
//    val parse =BaseAnalysis.parse("让战士们过一个欢乐祥和的新春佳节。")
//    parse.toArray.foreach(println)
//    val parse2 =BaseAnalysis.parse("让战士们过一个欢乐祥和的新春佳节。")
//    parse2.toArray.foreach(println)
// 构造一个用户词典

//    UserDefineLibrary.insertWord("ansj中文分词","userDefine",1000)
    UserDefineLibrary.insertWord("我是王婆","userDefine",1000)
  MyStaticValue.userLibrary="library/userLibrary/userLibrary.dic"
//    UserDefineLibrary.removeWord("ansj中文分词")
//   val path2= TextFenci.getClass.getResource("/")+"dic/userLibrary.dic"
//  val path=path2.substring(6)
//  var forest: Forest = Library.makeForest(path)
//  MyStaticValue.userLibrary="dic/userLibrary.dic"
//  println(path)
//  val FOREST:Forest = null;
//    UserDefineLibrary.loadLibrary(FOREST,path2)
//  UserDefineLibrary.loadFile(forest,new File(path))
//  UserDefineLibrary.loadLibrary(forest,"dic/userLibrary.dic")
//  MyStaticValue.userLibrary="/dic/userLibrary.dic"

    val trems1 = ToAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!")
  val list= ToAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!");
//  List<Term> parse = ToAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!");
// for ( a<-list){
//    a.
//  }
    FilterModifWord.modifResult(trems1)

    println(list)

  }
}
