package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.executor.{Partitioner, KeepPartitioner}
import org.nicta.wdy.hdm.functions.{ParMapFunc, ParFindByFunc, ParGroupByFunc, FindByKey}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/10.
 */
trait LogicalOptimizer extends Serializable {

  /**
   * optimize the structure of HDM
   *
   * @param cur  input HDM
   * @return     optimized HDM
   */
  def optimize[I:ClassTag, R:ClassTag](cur: HDM[I, R]): HDM[_, R]

}

/**
 *
 */
trait PhysicalOptimizer extends Serializable {

  def optimize(hdms: Seq[HDM[_, _]]): Seq[HDM[_, _]]

}


class FunctionFusion extends LogicalOptimizer with Logging{

  /**
   * optimize the structure of HDM
   *
   * @param cur  current HDM
   * @return     optimized HDM
   */
  override def optimize[I:ClassTag, R:ClassTag](cur: HDM[I, R]): HDM[_, R] = {
//    var cur = hdm
    if(cur.children == null) cur
    else {
      if((cur.dependency == OneToOne | cur.dependency == OneToN)
        && cur.children.forall(child => (child.dependency == OneToOne |  child.dependency == NToOne))){
         if(cur.children.size == 1) {
           val child = cur.children.head
           val first = child.asInstanceOf[HDM[child.inType.type, I]]
           val second = cur
           log.info(s"function fusion ${first.func} with ${second.func}")
           optimize(first.andThen(second))
         } else {
           val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[c.inType.type, I]]))
           cur.copy(children = seq)
         }
      } else {
        val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[c.inType.type, I]]))
        cur.copy(children = seq)
      }
    }

  }
}

class FilterLifting extends  LogicalOptimizer with Logging{

  /**
   * optimize the structure of HDM
   *
   * @param cur  input HDM
   * @return     optimized HDM
   */
  override def optimize[I: ClassTag, R: ClassTag](cur: HDM[I, R]): HDM[_, R] = {
    if(cur.children == null) cur
    else {
      if(cur.func.isInstanceOf[ParFindByFunc[_]] &&  cur.children.forall(child => child.func.isInstanceOf[ParMapFunc[_,_]])){
        val filterFunc = cur.func.asInstanceOf[ParFindByFunc[I]]
        val child = cur.children.head
        val mapFunc = child.func.asInstanceOf[ParMapFunc[child.inType.type ,I]]
        val nf = mapFunc.f.andThen(filterFunc.f)
        val newChildren = cur.children.map{ c =>
          c.children.map{_.asInstanceOf[HDM[_, child.inType.type]].filter(nf)}
        }.flatten
        log.info(s"Lift filter ${cur.func} in front of ${child.func} .")
        new DFM(children = newChildren.map(optimize(_)), func = mapFunc, dependency = cur.dependency, partitioner = cur.partitioner.asInstanceOf[Partitioner[I]]).asInstanceOf[HDM[_, R]]
      }
/*      if(cur.func.isInstanceOf[FindByKey[_,_]] &&  cur.children.forall(child => child.func.isInstanceOf[ParGroupByFunc[_,_]])){
        val fbk = cur.func.asInstanceOf[FindByKey[_,_]]
        val nfbk = cur.func.asInstanceOf[FindByKey[I,fbk.kType.type]]
        val head = cur.children.head
        val gb = head.func.asInstanceOf[ParGroupByFunc[I, fbk.kType.type]]
        val newChildren = cur.children.map{ c =>
          c.asInstanceOf[HDM[_,I]].filter( e => nfbk.f(gb.f(e))).copy(dependency = OneToOne, partitioner = new KeepPartitioner[I](1))
        }
        head.asInstanceOf[HDM[I,R]].copy(children = newChildren)
      }*/
      else {
        val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[c.inType.type, I]]))
        cur.copy(children = seq)
      }
    }
  }
}

object Optimizer {

 def combine[I:ClassTag, M:ClassTag, R:ClassTag](first:HDM[I,M], second:HDM[M, R] ):HDM[I,R] = {
    val cFunc = first.func.andThen(second.func)
    DFM(id = second.id, children = first.children, func = cFunc, partitioner = second.partitioner, parallelism = first.parallelism)
 }

}
