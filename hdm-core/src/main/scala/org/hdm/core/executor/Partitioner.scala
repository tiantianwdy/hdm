package org.hdm.core.executor

import org.hdm.core.collections.CompactBuffer
import org.hdm.core.functions.{RangePartitioning, TeraSortPartitioning}

import scala.collection._
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by Tiantian on 2014/5/26.
  */
trait Partitioner[T] extends Serializable {

  var partitionNum: Int

  val pFunc: T => Int

  def split(data: Seq[T]): Map[Int, _ <: Seq[T]]


  def split(data: Iterator[T]): Map[Int, _ <: Seq[T]] = split(data.toSeq)

}


class RandomPartitioner[T: ClassTag](var partitionNum: Int) extends Partitioner[T] {

  val pFunc: T => Int = null

  private val r = scala.util.Random

  override def split(data: Seq[T]): Map[Int, Seq[T]] = {

    val pData = data.groupBy(d => Math.abs(r.nextInt()) % partitionNum)
    pData
    //    mutable.Map.empty[Int,Seq[T]] ++= pData.map(seq => pData.indexOf(seq) -> seq)
  }
}

class HashPartitioner[T: ClassTag](var partitionNum: Int, val pFunc: T => Int) extends Partitioner[T] {


  override def split(data: Seq[T]): Map[Int, _ <: Seq[T]] = {

    val mapBuffer = new HashMap[Int, CompactBuffer[T]]()
    for (d <- data) {
      val partitionId = Math.abs(pFunc(d)) % partitionNum
      val bullet = mapBuffer.getOrElseUpdate(partitionId, CompactBuffer.empty[T])
      bullet += d
    }
    if (mapBuffer.keySet.size < partitionNum) {
      // if elems is less than the partition number return empty buffers
      for (i <- 0 until partitionNum) {
        if (!mapBuffer.contains(i)) {
          mapBuffer.put(i, CompactBuffer.empty[T])
        }
      }
    }
    mapBuffer

  }
}

class KeepPartitioner[T](var partitionNum: Int, val pFunc: T => Int = null) extends Partitioner[T] {

  override def split(data: Seq[T]): Map[Int, Seq[T]] = ???
}


class TeraSortPartitioner[T: ClassTag](partitionNum: Int) extends HashPartitioner[T](partitionNum, null) {

  val partitioning = new TeraSortPartitioning(partitionNum)

  override val pFunc = (d: T) => partitioning.partitionIndex(d)


}


class RangePartitioner[T: Ordering : ClassTag](bounds: Array[T]) extends HashPartitioner[T](bounds.length + 1, null) {

  val partitioning = new RangePartitioning[T](bounds)

  override val pFunc = (d: T) => partitioning.partitionIndex(d)
}


class RoundRobinPartitioner[T: ClassTag](var partitionNum: Int) extends Partitioner[T] {

  override val pFunc: (T) => Int = null

  override def split(data: Seq[T]): Map[Int, _ <: Seq[T]] = {
    val mapBuffer = new HashMap[Int, CompactBuffer[T]]()
    for (i <- 0 until data.size){
      val partitionId = i % partitionNum
      val buf = mapBuffer.getOrElseUpdate(partitionId, CompactBuffer.empty[T])
      buf += data(i)
    }
    mapBuffer
  }
}