//package org.nicta.wdy.hdm.planing
//
//import org.nicta.wdy.hdm.executor.Partitioner
//import org.nicta.wdy.hdm.functions._
//import org.nicta.wdy.hdm.model._
//
//import scala.reflect.ClassTag
//
///**
// * Created by tiantian on 1/03/16.
// */
//trait HDMPattern
//
//
//case class OneStepFurtherPattern[T: ClassTag, R: ClassTag, U: ClassTag](parent:HDM[R,U], current: HDM[T,R], children:Seq[HDM[_,T]]) extends HDMPattern
//
//
//
//trait OptimizingRule
//
//
//trait HDMRule extends OptimizingRule {
//
//
//  def pattern:PartialFunction[HDMPattern, Boolean]
//
//  def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern : OneStepFurtherPattern[T, R, U]):HDM[_, R]
//
//
//}
//
//object FunctionFusionRule extends HDMRule {
//
//  override def pattern =  {
//    case OneStepFurtherPattern(parent:HDM[_,_], current: HDM[_,_], children:Seq[HDM[_,_]]) =>
//      if ((current.dependency == OneToOne | current.dependency == OneToN)
//        && (children == null || children.isEmpty)) false
//      else  {
//        children.forall(hdm => hdm.dependency == OneToOne || hdm.dependency == NToOne)
//      }
//    case other:HDMPattern => false
//  }
//
//  override def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern: OneStepFurtherPattern[T, R, U]): HDM[_, R] = pattern match {
//    case OneStepFurtherPattern(parent, cur, children) => {
//      if (cur.children.size == 1 && !cur.children.head.isCache) {
//        val child = cur.children.head
//        val first = child
//        val second = cur
////        log.info(s"function fusion ${first.func} with ${second.func}")
//        first.andThen(second)
//      } else {
//        val childInTyp = children.head.inType
//        val seq = cur.children.map{c =>
//          apply(OneStepFurtherPattern(cur,
//            c.asInstanceOf[HDM[childInTyp.type, T]],
//            c.children.asInstanceOf[Seq[HDM[_, childInTyp.type]]]))
//        }
//        cur.copy(children = seq)
//      }
//    }
//  }
//}
//
//
//object FilterPushingOverMap extends HDMRule {
//
//
//  override def pattern: PartialFunction[HDMPattern, Boolean] = {
//    case OneStepFurtherPattern(parent, current, children) =>
//      if (children == null || children.isEmpty) false
//      else if (current.func.isInstanceOf[ParFindByFunc[_]]
//        && children.forall(hdm => hdm.func.isInstanceOf[ParMapFunc[_,_]])) true
//      else false
//    case other: HDMPattern => false
//  }
//
//  override def apply[T: ClassTag, R: ClassTag, U: ClassTag](pattern: OneStepFurtherPattern[T, R, U]): HDM[_, R] = pattern match {
//    case OneStepFurtherPattern(parent, cur, children) => {
//      val filter = cur.func.asInstanceOf[ParFindByFunc[R]]
//      val childTyp = children.head.inType
//      val newChildren = children.map{ child =>
//        val cfunc = child.func.asInstanceOf[ParMapFunc[childTyp.type, R]]
//        val newFilter = cfunc.f.andThen(filter.f)
//        child.children.map{ d =>
//          d.asInstanceOf[HDM[_, childTyp.type]].filter(newFilter).map(cfunc.f)
//        }
//      }.flatten
//      new DFM(children = newChildren,
//        func = new ParUnionFunc[R],
//        dependency = OneToOne,
//        partitioner = cur.partitioner)
//    }
//
//  }
//
//
//}
