package org.hdm.core.functions

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect._

/**
 * Created by tiantian on 26/10/15.
 *
 * Partitioning algorithm with sampling to decide the upper and lower bound
 *
 * @tparam K
 */
class RangePartitioning[K : Ordering : ClassTag](val boundaries:Array[K]) extends PartitioningFunc[K] {

  val numOfPartitions:Int = boundaries.length + 1

  private val ordering = implicitly[Ordering[K]]

  private val binarySearch: ((Array[K], K) => Int) = RangePartitioning.binarySearch[K]

  override def partitionIndex(key: K): Int = {
    binarySearch(boundaries, key)
  }

}

object RangePartitioning {

  def binarySearch[K : Ordering : ClassTag] : (Array[K], K) => Int = {
    classTag[K] match {
      case ClassTag.Float =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Float]], x.asInstanceOf[Float])
      case ClassTag.Double =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Double]], x.asInstanceOf[Double])
      case ClassTag.Byte =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Byte]], x.asInstanceOf[Byte])
      case ClassTag.Char =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Char]], x.asInstanceOf[Char])
      case ClassTag.Short =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Short]], x.asInstanceOf[Short])
      case ClassTag.Int =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Int]], x.asInstanceOf[Int])
      case ClassTag.Long =>
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[Long]], x.asInstanceOf[Long])
      case _ =>
        val comparator = implicitly[Ordering[K]].asInstanceOf[java.util.Comparator[AnyRef]]
        (l, x) => util.Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x.asInstanceOf[AnyRef], comparator)
    }
  }

  def decideBoundary[K : Ordering : ClassTag](input:Seq[K], numOfBoundary:Int):Array[K] = {
    import scala.collection.JavaConversions._
    val ordering = implicitly[Ordering[K]]
    val valMap = new util.HashMap[K, Int]()
    val sampledSize = input.length
    input foreach{ in =>
      if(!valMap.containsKey(in)) valMap.put(in, 1)
      else {
        val v = valMap.get(in)
        valMap.put(in, v + 1)
      }
    }
    val ordered = valMap.toSeq.map(d => d._1 -> (d._2.toDouble / sampledSize)).sortBy(_._1)
    val numCandidates = ordered.size
//    val sumWeights = ordered.map(_._2.toDouble / sampledSize).sum
    val step = 1D / numOfBoundary
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < numOfBoundary - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
