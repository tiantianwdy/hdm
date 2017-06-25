package org.hdm.core.planing

import org.hdm.core.model._
import org.hdm.core.server.HDMServerContext
import org.hdm.core.storage.HDMBlockManager

import scala.util.Try

/**
 * Created by Tiantian on 2014/12/10.
 */

/**
 *
 */
trait HDMPlaner extends Serializable {

  def plan(hdm:HDM[_], parallelism:Int):HDMPlans

}




trait DynamicPlaner {

  /**
   *
   * @param logicPlan
   * @return (nextTriggeredHDMs, remainingHDMs)
   */
  def planNext(logicPlan:Seq[ParHDM[_,_]]): (Seq[ParHDM[_,_]], Seq[ParHDM[_,_]]) = ???
}


/**
 *
 */
class StaticPlaner(hDMContext: HDMServerContext) extends HDMPlaner {

  val physicalPlaner = new DefaultPhysicalPlanner(HDMBlockManager(), isStatic = true, hDMContext)

  val logicalPlanner = new DefaultLocalPlaner(hDMContext.PLANER_PARALLEL_CPU_FACTOR, hDMContext.PLANER_PARALLEL_NETWORK_FACTOR)

  /**
   * ordered optimizers
   */
  val logicOptimizers: Seq[LogicalOptimizer] = Seq(
    new CacheOptimizer(),
    //    new FilterLifting(),
    new FunctionFusion()
  )

  val physicalOptimizers: Seq[PhysicalOptimizer] = Seq.empty[PhysicalOptimizer]

  override def plan(hdm: HDM[_], maxParallelism: Int): HDMPlans = {

      val explainedMap = new java.util.HashMap[String, HDM[_]]() // temporary map to save updated hdms
      var optimized:HDM[_] = hdm

      val originalFlow = logicalPlanner.plan(hdm, maxParallelism)

      // optimization
      logicOptimizers foreach { optimizer =>
        optimized = optimizer.optimize(optimized)
      }

      // logical planning
      val logicPlanOpt = logicalPlanner.plan(optimized, maxParallelism)

      //physical planning
      val physicalPlan =  logicPlanOpt.map { h =>
          h match {
            case dfm: ParHDM[_, _] =>
              val input = Try {
                h.children.map(cld => explainedMap.get(cld.id)).asInstanceOf[Seq[ParHDM[_, dfm.inType.type]]]
              } getOrElse {
                Seq.empty[ParHDM[_, dfm.inType.type]]
              }
              val newHdms = physicalPlaner.plan(input, h.asInstanceOf[ParHDM[dfm.inType.type, dfm.outType.type]], h.parallelism)
              newHdms.foreach(nh => explainedMap.put(nh.id, nh))
              newHdms

            case dfm : DualDFM[_, _, _] =>
              val input1 = Try {
                dfm.input1.map(cld => explainedMap.get(cld.id)).asInstanceOf[Seq[ParHDM[_, dfm.inType1.type]]]
              } getOrElse {
                Seq.empty[ParHDM[_, dfm.inType1.type]]
              }
              val input2 = Try {
                dfm.input2.map(cld => explainedMap.get(cld.id)).asInstanceOf[Seq[ParHDM[_, dfm.inType2.type]]]
              } getOrElse {
                Seq.empty[ParHDM[_, dfm.inType2.type]]
              }
              val newHdms = physicalPlaner.planMultiDFM(Seq(input1, input2), dfm, dfm.parallelism)
              newHdms.foreach(nh => explainedMap.put(nh.id, nh))
              newHdms
          }
        }.flatten
      //
      HDMPlans(originalFlow, logicPlanOpt, physicalPlan)
    }

}

