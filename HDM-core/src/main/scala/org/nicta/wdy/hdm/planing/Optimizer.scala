package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.model._

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

class FunctionFusion extends LogicalOptimizer {

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
           println(s"function fusion ${first.func} with ${second.func}")
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

object Optimizer {

 def combine[I:ClassTag, M:ClassTag, R:ClassTag](first:HDM[I,M], second:HDM[M, R] ):HDM[I,R] = {
    val cFunc = first.func.andThen(second.func)
    DFM(id = second.id, children = first.children, func = cFunc, partitioner = second.partitioner, parallelism = first.parallelism)
 }

}
