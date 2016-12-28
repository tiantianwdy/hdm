package org.hdm.core.functions

import com.google.common.primitives.{Bytes, Longs}

import scala.reflect.ClassTag

/**
 * Created by tiantian on 28/09/15.
 */
trait PartitioningFunc[T] extends  Serializable{

  def numOfPartitions:Int

  def partitionIndex(key:T):Int

}


/**
 * a generic partition algorithm with sorting based on first 7 bytes of the object
 * @param numOfPartitions
 * @param min
 * @param max
 */
class TeraSortPartitioning(val numOfPartitions:Int,
                        min:Long = TeraSortPartitioning.min,
                        max:Long = TeraSortPartitioning.max) extends PartitioningFunc[Any] {

  val rangePerPart = (max - min) / numOfPartitions

  override def partitionIndex(key: Any): Int = {
    val byte = key.asInstanceOf[Array[Byte]]
    val prefix = Longs.fromBytes(0, byte(0), byte(1), byte(2), byte(3), byte(4), byte(5), byte(6))
    (prefix / rangePerPart).toInt
  }
}


object TeraSortPartitioning {

  val min = Longs.fromBytes(0, 0, 0, 0, 0, 0, 0, 0)
  val max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1)
}


