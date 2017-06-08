package org.hdm.core.planing

import org.hdm.core.io.Path
import org.hdm.core.model.DDM

import scala.collection.mutable.Buffer

/**
 * Created by tiantian on 18/06/16.
 */
object DDMGroupUtils {//todo optimize performance and fix bugs that cause empty group

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
    while(elemIter.hasNext  && curGroup < groupNum -1) {
      val cur = elemIter.next()
      groupBuffer(curGroup) += cur
      groupSize(curGroup) += cur._2
      if(groupSize(curGroup) > (avgSize * (1F - approximation)) && curGroup < groupNum -1 ){
        curGroup += 1
      }
    }
    curGroup = 0
    while (elemIter.hasNext) {
      val next = elemIter.next()
      while(groupSize(curGroup) > avgSize) curGroup = (curGroup + 1) % groupNum
      groupBuffer(curGroup) += next
      groupSize(curGroup) += next._2
      curGroup = (curGroup + 1) % groupNum
    }
    groupBuffer.toSeq
  }

  def distributionGroup(ddmBuffer:Buffer[Buffer[(Path, Float)]], total:Float, group:Int, approximation:Float): Buffer[Buffer[(Path, Float)]] ={
    val finalRes = Buffer.empty[Buffer[(Path, Float)]]
    // subGrouping in each bounded group
    val distribution = ddmBuffer.map{ seq =>
      val seqSize = seq.map(_._2).sum
      Math.round((seqSize/total) * group)
    }
    for{
      i <- 0 until ddmBuffer.size
    }{
      val seq = ddmBuffer(i)
      val groupSize = Math.max(distribution(i), 1)
      finalRes ++= DDMGroupUtils.orderKeepingGroup(seq, groupSize, approximation)
    }
    finalRes
  }

  def cutBuffer(buf:Buffer[(Path, Float)], targetWeight:Float, total:Float):Buffer[(Path, Float)] = {
    val cutBuffer = Buffer.empty[(Path, Float)]
    var next = buf.last
    var curWeight = total
    while(curWeight > targetWeight){
      buf -= next
      cutBuffer += next
      curWeight -= next._2
      next = buf.last
    }
    cutBuffer
  }

  def mergeNeighbours(ddmBuffer:Buffer[Buffer[(Path, Float)]], total:Float, group:Int): Buffer[Buffer[(Path, Float)]] = { //todo optimize

    require(ddmBuffer.length >= group)

    val finalRes = Buffer.empty[Buffer[(Path, Float)]]
    val avg = total / group
    val bufSize = ddmBuffer.length
    val weightsVector = ddmBuffer.map( buf => buf.map(_._2).sum)
    val sortedWeights = weightsVector.zipWithIndex

    // take top k = group from ddmBuffer
    val sorting = (w1:(Float, Int), w2:(Float, Int)) => w1._1 > w2._1
    sortedWeights.sortWith(sorting)
    val (resIdxes, bufferIdxes) = sortedWeights.splitAt(group)
    val resBuffer = resIdxes.map(t => ddmBuffer(t._2))
    val resWeights = resIdxes.map(t => weightsVector(t._2))
    val elemBuffer = bufferIdxes.map(t => ddmBuffer(t._2)).flatten

    // check the top K buffer
    var i = 0
    while(i < resIdxes.length){
      val bufIdx = resIdxes(i)._2
      val buf = ddmBuffer(bufIdx)
      val weights = weightsVector(bufIdx)
      if(weights > avg){
        val cutBuf = cutBuffer(buf, avg, weights)
        if(cutBuf.nonEmpty) elemBuffer ++= cutBuf
        // move completed group to final buffer
        resBuffer -= buf
        finalRes += buf
        resWeights(i) = -1
      }
      i += 1
    }

    // balance non-completed groups
    var resIdx =  resBuffer.size - 1
    var elemIdx = 0
    val groupWeightVec = resWeights.filter(_ >= 0)
    while(resIdx >= 0 && elemIdx < elemBuffer.size){
      val elem = elemBuffer(elemIdx)
      val amount = elem._2
      while(groupWeightVec(resIdx) + amount > avg && resIdx >= 0) {
        resIdx -= 1
      }
      if( resIdx >= 0 ){
        resBuffer(resIdx) += elem
        groupWeightVec(resIdx) += amount
        elemIdx += 1
      }
    }
    finalRes ++= resBuffer
    resIdx = 0
    while(elemIdx < elemBuffer.size){
      finalRes(resIdx) += elemBuffer(elemIdx)
      resIdx = (resIdx +1) % finalRes.size
    }

//
//
//    var curWeight = 0
//    var curRes = Buffer.empty[(Path, Float)]
//    var curIdx = 0
//    var curBuffer = ddmBuffer(curIdx)
//    while(curIdx < bufSize && finalRes.length < group) {
//      while(curWeight < avg && curBuffer.size > 0){
//        val next = curBuffer.remove(0)
//        curRes += next
//        curWeight += next._2
//      }
//      if(curWeight > avg){
//        finalRes += curRes
//        curRes = Buffer.empty[(Path, Float)]
//        curWeight = 0
//      }
//      if(curBuffer.size <= 0){
//        curIdx += 1
//        curBuffer = ddmBuffer(curIdx)
//      }
//    }

    finalRes
  }

  def groupPathByBoundary(paths:Seq[Path], weights:Seq[Float], n:Int, boundary:Int): Buffer[Buffer[(Path, Float)]] = {
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
        if ((Path.path2Int(next._1) - Path.path2Int(cur._1)) > boundary ){
          ddmBuffer += buffer
          buffer = Buffer.empty[(Path, Float)] += next
        } else {
          buffer += next
        }
        cur = next
      }
      ddmBuffer += buffer
    }
    println(s"${ddmBuffer.size},$total, $n ")
    distributionGroup(ddmBuffer, total, n, approximation)
//    mergeNeighbours(ddmBuffer, total, n)
  }

  def groupDDMByBoundary[R](ddms:Seq[DDM[_, R]], weights:Seq[Float], n:Int, boundary:Int = (256 << 8) -1 ) ={
    val ddmMap = ddms.map(d => (d.preferLocation -> d)).toMap[Path, DDM[_,R]]
    val paths = ddms.map(_.preferLocation)
    val grouped = groupPathByBoundary(paths, weights, n, boundary)
    grouped.map{seq =>
      seq.map(p => ddmMap(p._1))
    }
  }

}
