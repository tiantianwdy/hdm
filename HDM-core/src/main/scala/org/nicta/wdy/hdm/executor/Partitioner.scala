package org.nicta.wdy.hdm.executor

import java.util

import org.nicta.wdy.hdm.collections.CompactBuffer
import org.nicta.wdy.hdm.functions.{RangePartitioning, TeraSortPartitioning}

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


  def split(data: Iterator[T]):Map[Int, _ <: Seq[T]] = split(data.toSeq)

}


class RandomPartitioner[T:ClassTag](var partitionNum:Int) extends  Partitioner[T] {

  val pFunc: T => Int = null

  override def split(data: Seq[T]): Map[Int,Seq[T]] = {

    val pData = data.grouped(partitionNum).toList
    mutable.Map.empty[Int,Seq[T]] ++= pData.map(seq => pData.indexOf(seq) -> seq)
  }
}

class HashPartitioner[T:ClassTag] (var partitionNum:Int, val pFunc: T => Int ) extends  Partitioner[T] {


  override def split(data: Seq[T]): Map[Int, _ <: Seq[T]] = {

    val mapBuffer = new HashMap[Int, CompactBuffer[T]]()
    for (d <- data) {
      val partitionId = Math.abs(pFunc(d)) % partitionNum
      val bullet = mapBuffer.getOrElseUpdate(partitionId, CompactBuffer.empty[T])
      bullet += d
    }
    if(mapBuffer.keySet.size < partitionNum){ // if elems is less than the partition number return empty buffers
      for(i <- 0 until partitionNum){
        if(!mapBuffer.contains(i)){
          mapBuffer.put(i, CompactBuffer.empty[T])
        }
      }
    }
    mapBuffer

  }
}

class KeepPartitioner[T](var partitionNum:Int , val pFunc: T => Int = null) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int, Seq[T]] = ???
}


class TeraSortPartitioner[T: ClassTag] (partitionNum:Int) extends HashPartitioner[T](partitionNum, null){

  val partitioning = new TeraSortPartitioning(partitionNum)

  override val pFunc = (d:T) => partitioning.partitionIndex(d)


}


class RangePartitioner[T: Ordering: ClassTag] (bounds:Array[T]) extends HashPartitioner[T](bounds.length + 1, null){

  val partitioning = new RangePartitioning[T](bounds)

  override val pFunc = (d:T) => partitioning.partitionIndex(d)
}