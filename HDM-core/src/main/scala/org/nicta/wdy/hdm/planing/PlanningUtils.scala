package org.nicta.wdy.hdm.planing

/**
 * Created by tiantian on 9/04/15.
 */

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.model.DDM
import org.nicta.wdy.hdm.scheduling.{MinMinScheduling, SchedulingTask}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable.Buffer
import org.nicta.wdy.hdm.io.Path

object PlanningUtils {

  def simpleGrouping[T](elem:Seq[T], groupNum:Int):Seq[Seq[T]]= {
    elem.grouped(elem.size/groupNum).toSeq
  }

  def orderKeepingGroup[T](elem:Seq[T], groupNum:Int):Seq[Buffer[T]]= {
    val elemBuffer = elem.toBuffer
    val groupBuffer = Array.fill(groupNum){Buffer.empty[T]}
    val elemSize = elem.size/groupNum
    for (g <- 0 until groupNum){
      groupBuffer(g) = elemBuffer.take(elemSize)
      elemBuffer.remove(0, elemSize)
    }
    for (elem <- elemBuffer){
      val idx = elemBuffer.indexOf(elem) % groupNum
      groupBuffer(idx) += elem
    }
    groupBuffer.toSeq
  }



  def seqSlide[T](elem:Seq[T], slideLen:Int):Seq[T] ={
    val (pre, post) = elem.splitAt(slideLen)
    post ++ pre
  }

  def groupPathBySimilarity(paths:Seq[Path], n:Int) = {
    val avg = paths.size/n
    paths.sortWith( (p1,p2) => Path.path2Int(p1) < Path.path2Int(p2)).grouped(avg.toInt).toSeq
  }

  def groupDDMByLocation(ddms:Seq[DDM[String,String]], n:Int) = {
    //    val avg = ddms.size/n
    //    ddms.sortWith( (p1,p2) => path2Int(p1.preferLocation) < path2Int(p2.preferLocation)).grouped(avg.toInt).toSeq
    val ddmMap = ddms.map(d => (d.preferLocation -> d)).toMap
    val paths = ddms.map(_.preferLocation)
    val grouped = groupPathBySimilarity(paths, n)
    grouped.map{seq =>
      seq.map(p => ddmMap(p))
    }
  }

  def groupPathByBoundary(paths:Seq[Path], n:Int, boundary:Int = 256 << 8 ) = {
    val sorted = paths.sortWith( (p1,p2) => Path.path2Int(p1) < Path.path2Int(p2)).iterator
//    val boundary = 256 << 8 - 1
    val ddmBuffer = Buffer.empty[Buffer[Path]]
    var buffer = Buffer.empty[Path]
    val total = paths.size.toFloat

    if(sorted.hasNext){
      var cur = sorted.next()
      buffer += cur
      while (sorted.hasNext) {
        val next = sorted.next()
        if ((Path.path2Int(next) - Path.path2Int(cur)) > boundary ) {
          ddmBuffer += buffer
          buffer = Buffer.empty[Path] += next
        } else {
          buffer += next
        }
        cur = next
      }
      ddmBuffer += buffer
    }

    println(s"${ddmBuffer.size},$total, $n ")
    // subGrouping in each bounded group
    val distribution = ddmBuffer.map(seq => Math.round( (seq.size/total) * n))
    val finalRes = Buffer.empty[Buffer[Path]]
    for{
      i <- 0 until ddmBuffer.size
    }{
      val seq = ddmBuffer(i)
      val groupSize = distribution(i)
      finalRes ++= PlanningUtils.orderKeepingGroup(seq, groupSize)
    }
    finalRes
  }

  def groupDDMByBoundary[R](ddms:Seq[DDM[_, R]], n:Int, boundary:Int) ={
    val ddmMap = ddms.map(d => (d.preferLocation -> d)).toMap[Path, DDM[_,R]]
    val paths = ddms.map(_.preferLocation)
    val grouped = groupPathByBoundary(paths, n, boundary)
    grouped.map{seq =>
      seq.map(p => ddmMap(p))
    }
  }

  def groupDDMByMinminScheduling[R](ddms: Seq[DDM[_, R]], candidates: Seq[Path], hDMContext: HDMContext) = {
    val ddmMap = ddms.map(d => (d.id -> d)).toMap[String, DDM[_, R]]
    val tasks = ddms.map { ddm =>
      val id = ddm.id
      val inputLocations = Seq(ddm.preferLocation)
      val inputSize = Seq(ddm.blockSize / 1024)
      SchedulingTask(id, inputLocations, inputSize, ddm.dependency)
    }
    val policy = new MinMinScheduling
    val plans = policy.plan(tasks, candidates,
      hDMContext.SCHEDULING_FACTOR_CPU,
      hDMContext.SCHEDULING_FACTOR_IO,
      hDMContext.SCHEDULING_FACTOR_NETWORK)
    val grouped = plans.toSeq.groupBy(_._2).map(group => group._2.map(kv => ddmMap(kv._1))).toSeq
    grouped
  }
}
