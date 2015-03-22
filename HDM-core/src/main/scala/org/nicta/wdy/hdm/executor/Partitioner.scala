package org.nicta.wdy.hdm.executor

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection._

/**
 * Created by Tiantian on 2014/5/26.
 */
trait Partitioner[T] extends Serializable{

  var partitionNum:Int

  val pFunc: T => Int

  def split(data: Seq[T]):Map[Int, _ <: Seq[T]]

}


class RandomPartitioner[T](var partitionNum:Int, val pFunc: T => Int = null ) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int,Seq[T]] = {

    val pData = data.grouped(partitionNum).toList
    mutable.Map.empty[Int,Seq[T]] ++= pData.map(seq => pData.indexOf(seq) -> seq)
  }
}

class MappingPartitioner[T] (var partitionNum:Int, val pFunc: T => Int ) extends  Partitioner[T] {

 import scala.collection.JavaConversions._

  override def split(data: Seq[T]): Map[Int, _ <: Seq[T]] = {

    val mapBuffer = new HashMap[Int, ArrayBuffer[T]]()
    for (d <- data) {
      val k = Math.abs(pFunc(d)) % partitionNum
      val bullet = mapBuffer.getOrElseUpdate(k, ArrayBuffer.empty[T])
      bullet += d
    }
    mapBuffer

//    data.groupBy(d=> Math.abs(pFunc(d)) % partitionNum)
  }
}

class KeepPartitioner[T](var partitionNum:Int , val pFunc: T => Int = null) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int, Seq[T]] = ???
}
