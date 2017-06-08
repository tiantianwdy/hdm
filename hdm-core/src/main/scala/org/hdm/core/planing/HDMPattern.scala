package org.hdm.core.planing

import org.hdm.core.executor.Partitioner
import org.hdm.core.functions._
import org.hdm.core.model._
import org.hdm.core.utils.Logging

import scala.reflect.ClassTag

/**
* Created by tiantian on 1/03/16.
*/
trait HDMPattern


case class RadiusOnePattern[T: ClassTag, R: ClassTag, U: ClassTag](parent:HDM[U], current: HDM[R], children:Seq[HDM[T]]) extends HDMPattern



trait OptimizingRule


trait HDMRule extends OptimizingRule {


  def matchPattern:PartialFunction[HDMPattern, Boolean]

  def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern : RadiusOnePattern[T, R, U]):HDM[R]


}

object FunctionFusionRule extends HDMRule with Logging{

  override def matchPattern =  {
    case RadiusOnePattern(parent:ParHDM[_,_], current: ParHDM[_,_], children:Seq[ParHDM[_,_]]) =>
      if ((current.dependency == OneToOne | current.dependency == OneToN)
        && (children == null || children.isEmpty)) false
      else  {
        children.forall(hdm => hdm.dependency == OneToOne || hdm.dependency == NToOne)
      }
    case other:HDMPattern => false
  }

  override def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern: RadiusOnePattern[T, R, U]): HDM[R] = pattern match {
    case RadiusOnePattern(parent, cur, children) => {
      cur match {
        case curHDM: ParHDM[_, R] =>
          if (cur.children == null || cur.children.isEmpty) cur
          else {
            if ((cur.dependency == OneToOne | cur.dependency == OneToN)
              && cur.children.forall(child => (child.dependency == OneToOne | child.dependency == NToOne))) {
              if (cur.children.size == 1 && !cur.children.head.isCache) {
                val child = curHDM.children.head
                val first = child.asInstanceOf[HDM[child.outType.type]]
                val second = curHDM.asInstanceOf[ParHDM[child.outType.type, R]]
                log.info(s"function fusion ${first.func} with ${second.func}")
                first.andThen(second)
              } else {
                val seq = cur.children.map(c => c.asInstanceOf[HDM[curHDM.inType.type]])
                curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
              }
            } else {
              val seq = cur.children.map(c => c.asInstanceOf[HDM[curHDM.inType.type]])
              curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
            }
          }

        case dualHDM:DualDFM[_, _, R] =>
          val input1 = dualHDM.input1.map(c => c.asInstanceOf[HDM[dualHDM.inType1.type]])
          val input2 = dualHDM.input2.map(c => c.asInstanceOf[HDM[dualHDM.inType2.type]])
          dualHDM.asInstanceOf[DualDFM[dualHDM.inType1.type, dualHDM.inType2.type, R]].copy(input1 = input1, input2 = input2)

      }
    }
  }
}


object FilterPushingOverMap extends HDMRule with Logging{


  override def matchPattern: PartialFunction[HDMPattern, Boolean] = {
    case RadiusOnePattern(parent, current, children) =>
      if (children == null || children.isEmpty) false
      else if (current.func.isInstanceOf[ParFindByFunc[_]]
        && children.forall(hdm => hdm.func.isInstanceOf[ParMapFunc[_,_]])) true
      else false
    case other: HDMPattern => false
  }

  override def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern: RadiusOnePattern[T, R, U]): HDM[R] = pattern match {
    case RadiusOnePattern(parent, cur, children) => cur match {
      case curHDM: ParHDM[_, R] =>
        if (cur.children == null) cur
        else {
          if (cur.func.isInstanceOf[ParFindByFunc[_]] && cur.children.forall(child => child.func.isInstanceOf[ParMapFunc[_, _]])) {
            val filterFunc = cur.func.asInstanceOf[ParFindByFunc[R]]
            val child = cur.children.head.asInstanceOf[ParHDM[_, R]]
            val mapFunc = child.func.asInstanceOf[ParMapFunc[child.inType.type, R]]
            val nf = mapFunc.f.andThen(filterFunc.f)
            val newChildren = cur.children.map { c =>
              c.children.map {
                _.asInstanceOf[ParHDM[_, child.inType.type]].filter(nf)
              }
            }.flatten
            log.info(s"Lift filter ${cur.func} in front of ${child.func} .")
            new DFM(children = newChildren, func = mapFunc, dependency = cur.dependency, partitioner = cur.partitioner, location = cur.location, appContext = cur.appContext).asInstanceOf[ParHDM[_, R]]
          } else {
            val seq = cur.children.map(c => c.asInstanceOf[HDM[curHDM.inType.type]])
            curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
          }
        }

      case dualHDM:DualDFM[_, _, R] => dualHDM
    }

  }


}

/**
 * Deep first optimizer which applies optimization rules in a deep-first manner on the HDM tree
 */
class DFOptimizer {

  def applyRule[R: ClassTag, U: ClassTag](parent:HDM[U], hdm:HDM[R], hDMRule: HDMRule):HDM[R] = {
    hdm match {
      case cur:ParHDM[_, R] =>
        if(cur.children ne null){
          val nChildren = cur.children.map(_.asInstanceOf[HDM[cur.inType.type]]).map{child =>
            applyRule(cur, child, hDMRule)
          }.toSeq
          optimize(parent, cur.asInstanceOf[ParHDM[cur.inType.type, R]].copy(children = nChildren), hDMRule)
        } else {
          cur
        }
      case dualHDM:DualDFM[_, _, R] =>
        val inputOne = if(dualHDM.input1 ne null){
          dualHDM.input1.map(_.asInstanceOf[HDM[dualHDM.inType1.type]]).map(c => applyRule(dualHDM, c, hDMRule))
        } else {
          dualHDM.input1.asInstanceOf[Seq[HDM[dualHDM.inType1.type]]]
        }
        val input2 = if(dualHDM.input2 ne null) {
          dualHDM.input2.map(_.asInstanceOf[HDM[dualHDM.inType2.type]]).map(c => applyRule(dualHDM, c, hDMRule))
        } else {
          dualHDM.input2.asInstanceOf[Seq[HDM[dualHDM.inType2.type]]]
        }
        optimize(parent, dualHDM.asInstanceOf[DualDFM[dualHDM.inType1.type, dualHDM.inType2.type, R]].copy(input1 = inputOne, input2 = input2), hDMRule)
    }
  }


  def optimize[R: ClassTag, U : ClassTag](parent:HDM[U], hdm:HDM[R], hDMRule: HDMRule):HDM[R] = {
    val pattern = RadiusOnePattern(parent, hdm, hdm.children.asInstanceOf[Seq[HDM[Any]]])
    if(hDMRule.matchPattern(pattern)){
      hDMRule.apply(pattern)
    } else {
      hdm
    }
  }


}
