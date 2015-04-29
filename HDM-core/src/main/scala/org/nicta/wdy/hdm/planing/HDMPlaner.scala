package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.functions.{FlattenFunc, ParUnionFunc, ParallelFunction}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Tiantian on 2014/12/10.
 */

/**
 *
 */
trait HDMPlaner extends Serializable {

  def plan(hdm:HDM[_,_], parallelism:Int):Seq[HDM[_,_]]

}


trait DynamicPlaner {

  /**
   *
   * @param logicPlan
   * @return (nextTriggeredHDMs, remainingHDMs)
   */
  def planNext(logicPlan:Seq[HDM[_,_]]): (Seq[HDM[_,_]], Seq[HDM[_,_]]) = ???
}


/**
 *
 */

object StaticPlaner extends HDMPlaner{

  val physicalPlaner = new DefaultPhysicalPlanner(HDMBlockManager(), isStatic = true)
  
  val logicalPlanner = new DefaultLocalPlaner

  /**
   * ordered optimizers
   */
  val logicOptimizers:Seq[LogicalOptimizer] = Seq{
    new FunctionFusion()
  }

  val physicalOptimizers:Seq[PhysicalOptimizer] = Seq.empty[PhysicalOptimizer]

  override def plan(hdm:HDM[_,_], maxParallelism:Int):Seq[HDM[_,_]] = {
    val explainedMap  = new java.util.HashMap[String, HDM[_,_]]()// temporary map to save updated hdms
    var optimized = hdm
    // optimization
    logicOptimizers foreach { optimizer =>
      optimized = optimizer.optimize(hdm)
    }
    // logical planning
    val logicPlan = logicalPlanner.plan(optimized, maxParallelism)
    //physical planning
    logicPlan.map{ h =>
      val input = Try {h.children.map(cld => explainedMap.get(cld.id)).asInstanceOf[Seq[HDM[_,h.inType.type]]]} getOrElse {Seq.empty[HDM[_,h.inType.type]]}
      val newHdms = physicalPlaner.plan(input, h.asInstanceOf[HDM[h.inType.type, h.outType.type ]], h.parallelism)
      newHdms.foreach(nh => explainedMap.put(nh.id, nh))
      newHdms
    }
  }.flatten
}
