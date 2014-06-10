package org.nicta.unsw.ml

import scala.io.Source
import scala.util.Try

/**
 * Created by Dongyao.Wu on 2014/5/28.
 */
object DataParser {

  def toDouble(str:String):Double = Try{str.replaceAll("[^\\+-1234567890.]+","")toDouble} getOrElse (0D)

  def toDouble(any :Any):Double = Try{toDouble(any.toString)} getOrElse (0D)

  def parseCsv(path:String, regex:String, keyIdx:Int, contentIdx:Int):Map[String,Double] = {
    val data = Source.fromFile(path,"utf-8")
    val maxIdx = math.max(keyIdx, contentIdx)
    val dataArr = data.getLines().map(_.split(regex) match {
      case arr :Array[String] if arr.length > maxIdx =>
//        println(arr(contentIdx) + " -> " +  toDouble(arr(contentIdx)))
        Some(arr(keyIdx) -> toDouble(arr(contentIdx)))
      case _ => None
    }).toSeq.filter(_ ne None).map(_.get)
    dataArr.toMap
  }

  def parseCsv(path:String, regex:String, keyIdx:Int, contentIdxs:Seq[Int]):Map[String,Seq[Double]] = {
    val data = Source.fromFile(path,"UTF-8")
    val maxIdx = (contentIdxs :+ keyIdx).reduceLeft(math.max(_,_))
    val dataArr = data.getLines().map(_.split(regex) match {
      case arr: Array[String] if arr.length > maxIdx => Some(arr(keyIdx) -> contentIdxs.map(idx => toDouble(arr(idx))))
      case _ => None
    }).toSeq.filter(_ ne None).map(_.get)
    dataArr.toMap
  }

  def parseCsv(path:String, regex:String, keyIdx:Int, range:Range):Map[String,Seq[Double]] = {
      parseCsv(path, regex, keyIdx, range.toList)
  }


}
