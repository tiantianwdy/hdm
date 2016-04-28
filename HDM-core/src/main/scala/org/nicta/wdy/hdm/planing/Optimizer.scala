package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.executor.{Partitioner, KeepPartitioner}
import org.nicta.wdy.hdm.functions.{ParMapFunc, ParFindByFunc, ParGroupByFunc, FindByKey}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
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
  def optimize[R:ClassTag](cur: HDM[R]): HDM[R]

}

/**
 *
 */
trait PhysicalOptimizer extends Serializable {

  def optimize(hdms: Seq[ParHDM[_, _]]): Seq[ParHDM[_, _]]

}


class FunctionFusion extends LogicalOptimizer with Logging{

  /**
   * optimize the structure of HDM
   *
   * @param cur  current HDM
   * @return     optimized HDM
   */
  override def optimize[R: ClassTag](cur: HDM[R]): HDM[R] = {
    //    var cur = hdm
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
              optimize(first.andThen(second))
            } else {
              val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[curHDM.inType.type]]))
              curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
            }
          } else {
            val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[curHDM.inType.type]]))
            curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
          }
        }

      case dualHDM:DualDFM[_, _, R] =>
        val input1 = dualHDM.input1.map(c => optimize(c.asInstanceOf[HDM[dualHDM.inType1.type]]))
        val input2 = dualHDM.input2.map(c => optimize(c.asInstanceOf[HDM[dualHDM.inType2.type]]))
        dualHDM.asInstanceOf[DualDFM[dualHDM.inType1.type, dualHDM.inType2.type, R]].copy(input1 = input1, input2 = input2)

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
  override def optimize[R: ClassTag](cur: HDM[R]): HDM[R] = {
    cur match {
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
            new DFM(children = newChildren.map(optimize(_)), func = mapFunc, dependency = cur.dependency, partitioner = cur.partitioner, location = cur.location, appContext = cur.appContext).asInstanceOf[ParHDM[_, R]]
          } else {
            val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[curHDM.inType.type]]))
            curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
          }
        }
//    case dualHDM:DualDFM[_,_,_] =>
    }
  }
}

/**
 * optimizer which replace cached hdm in the data flow
 */
class CacheOptimizer extends  LogicalOptimizer with Logging{

  /**
   * optimize the structure of HDM
   *
   * @param cur  input HDM
   * @return     optimized HDM
   */
  override def optimize[R: ClassTag](cur: HDM[R]): HDM[R] = {
    if (cur.isCache && HDMBlockManager().checkState(cur.id, Computed)) {
      val cached = HDMBlockManager().getRef(cur.id).asInstanceOf[ParHDM[_, R]]
      log.info(s"Replace HDM ${cur} with cached: ${cached} .")
      cached
    } else if(cur.children == null) {
      cur
    } else {
      cur match {
        case curHDM: ParHDM[_, R] =>
          val seq = cur.children.map(c => optimize(c.asInstanceOf[HDM[curHDM.inType.type]]))
          curHDM.asInstanceOf[ParHDM[curHDM.inType.type, R]].copy(children = seq)
        case dualHDM:DualDFM[_, _, R] =>
          val input1 = dualHDM.input1.map(c => optimize(c.asInstanceOf[HDM[dualHDM.inType1.type]]))
          val input2 = dualHDM.input2.map(c => optimize(c.asInstanceOf[HDM[dualHDM.inType2.type]]))
          dualHDM.asInstanceOf[DualDFM[dualHDM.inType1.type, dualHDM.inType2.type, R]].copy(input1 = input1, input2 = input2)
      }
    }
  }

}

object Optimizer {

 def combine[I:ClassTag, M:ClassTag, R:ClassTag](first:ParHDM[I,M], second:ParHDM[M, R] ):ParHDM[I,R] = {
    val cFunc = first.func.andThen(second.func)
    DFM(id = second.id, children = first.children, func = cFunc, partitioner = second.partitioner, parallelism = first.parallelism, location = first.location, appContext = first.appContext)
 }

}
