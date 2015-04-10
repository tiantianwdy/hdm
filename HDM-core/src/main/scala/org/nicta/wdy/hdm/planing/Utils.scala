package org.nicta.wdy.hdm.planing

/**
 * Created by tiantian on 9/04/15.
 */
import scala.collection.mutable.Buffer
object Utils {

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

}
