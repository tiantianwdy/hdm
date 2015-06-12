package org.nicta.wdy.hdm.executor

import java.util

import org.nicta.wdy.hdm.collections.CompactBuffer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection._
import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/5/26.
 */
trait Partitioner[T] extends Serializable{

  var partitionNum:Int

  val pFunc: T => Int

  def split(data: Seq[T]):Map[Int, _ <: Seq[T]]

}


class RandomPartitioner[T:ClassTag](var partitionNum:Int, val pFunc: T => Int = null ) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int,Seq[T]] = {

    val pData = data.grouped(partitionNum).toList
    mutable.Map.empty[Int,Seq[T]] ++= pData.map(seq => pData.indexOf(seq) -> seq)
  }
}

class HashPartitioner[T:ClassTag] (var partitionNum:Int, val pFunc: T => Int ) extends  Partitioner[T] {

 import scala.collection.JavaConversions._

  override def split(data: Seq[T]): Map[Int, _ <: Seq[T]] = {

    val mapBuffer = new HashMap[Int, CompactBuffer[T]]()
    for (d <- data) {
      val k = Math.abs(pFunc(d)) % partitionNum
      val bullet = mapBuffer.getOrElseUpdate(k, CompactBuffer.empty[T])
      bullet += d
    }
    mapBuffer

  }
}

class KeepPartitioner[T](var partitionNum:Int , val pFunc: T => Int = null) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int, Seq[T]] = ???
}
