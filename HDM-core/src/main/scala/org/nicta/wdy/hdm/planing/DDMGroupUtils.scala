package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.DDM

import scala.collection.mutable.Buffer

/**
 * Created by tiantian on 18/06/16.
 */
object DDMGroupUtils {

  def weightedGroup[T](elems:Seq[T], weights:Array[Float], groupNum:Int):Seq[Buffer[T]]= {
    require(elems.length == weights.length)
    val groupBuffer = Array.fill(groupNum){Buffer.empty[T]}
    val totalWeights = weights.sum
    val weightPerGroup = totalWeights / groupNum
    var accumulatedWeights = 0F
    var groupIdx = 0
    for(idx <- 0 until elems.length){
      val cur = elems(idx)
      if(accumulatedWeights < weightPerGroup){
        groupBuffer(idx) += cur
        accumulatedWeights += weights(idx)
      } else {
        accumulatedWeights = 0
        groupIdx += 1
      }
    }
    groupBuffer.toSeq
  }

  def orderKeepingGroup(elem:Seq[(Path, Float)], groupNum:Int, approximation:Float):Seq[Buffer[(Path, Float)]]= {
    val elemIter = elem.iterator
    val groupBuffer = Array.fill(groupNum){Buffer.empty[(Path, Float)]}
    val groupSize = Array.fill(groupNum){0F}
    val avgSize = elem.map(_._2).sum / groupNum
    var curGroup = 0
    while(elemIter.hasNext  && curGroup < groupNum -1){
      val cur = elemIter.next()
      groupBuffer(curGroup) += cur
      groupSize(curGroup) += cur._2
      if(groupSize(curGroup) > (avgSize * (1F - approximation)) && curGroup < groupNum -1 ){
        curGroup += 1
      }
    }
    curGroup = 0
    while (elemIter.hasNext){
      val next = elemIter.next()
      while(groupSize(curGroup) > avgSize) curGroup = (curGroup + 1) % groupNum
      groupBuffer(curGroup) += next
      groupSize(curGroup) += next._2
      curGroup = (curGroup + 1) % groupNum
    }
    groupBuffer.toSeq
  }


  def groupPathByBoundary(paths:Seq[Path], weights:Seq[Float], n:Int, boundary:Int) = {
    require(paths.length == weights.length)
    val tuples = paths.zip(weights)
    val sorted = tuples.sortWith( (p1,p2) => Path.path2Int(p1._1) < Path.path2Int(p2._1)).iterator
    val ddmBuffer = Buffer.empty[Buffer[(Path, Float)]]
    var buffer = Buffer.empty[(Path, Float)]
    val total = weights.sum
    val approximation = 0.2F

    if(sorted.hasNext){
      var cur = sorted.next()
      buffer += cur
      while (sorted.hasNext) {
        val next = sorted.next()
        if ((Path.path2Int(next._1) - Path.path2Int(cur._1)) >= boundary ){
          ddmBuffer += buffer
          buffer = Buffer.empty[(Path, Float)] += next
        } else {
          buffer += next
        }
        cur = next
      }
      ddmBuffer += buffer
    }
    // subGrouping in each bounded group
    val distribution = ddmBuffer.map{ seq =>
      val seqSize = seq.map(_._2).sum
      Math.round((seqSize/total) * n)
    }
    val finalRes = Buffer.empty[Buffer[(Path, Float)]]
    for{
      i <- 0 until ddmBuffer.size
    }{
      val seq = ddmBuffer(i)
      val groupSize = distribution(i)
      finalRes ++= DDMGroupUtils.orderKeepingGroup(seq, groupSize, approximation)
    }
    finalRes
  }

  def groupDDMByBoundary[R](ddms:Seq[DDM[_, R]], weights:Seq[Float], n:Int, boundary:Int = 256 << 8 + 256) ={
    val ddmMap = ddms.map(d => (d.preferLocation -> d)).toMap[Path, DDM[_,R]]
    val paths = ddms.map(_.preferLocation)
    val grouped = groupPathByBoundary(paths, weights, n, boundary)
    grouped.map{seq =>
      seq.map(p => ddmMap(p._1))
    }
  }

}
