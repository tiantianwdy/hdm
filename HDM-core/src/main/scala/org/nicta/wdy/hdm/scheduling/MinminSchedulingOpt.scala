package org.nicta.wdy.hdm.scheduling

import java.util

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable

/**
 * Created by tiantian on 6/07/16.
 */
class MinminSchedulingOpt extends SchedulingPolicy with  Logging{

  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data, normally computeFactor >> ioFactor >= networkFactor
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return              pairs of planed tasks: (taskID -> workerPath)
   */
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): mutable.Map[String, String] = {
    val taskBuffer = inputs.toBuffer
    val results = mutable.Map.empty[String, String]

//    while(taskBuffer.nonEmpty) {
      if (taskBuffer.nonEmpty && resources.nonEmpty) {
        val jobSize = taskBuffer.length
        val workerSize = resources.length
        val costMatrix = Array.fill[Array[Double]](jobSize) {
          Array.fill(workerSize) {
            0D
          }
        }
        val timeMatrix = new Array[Array[Double]](jobSize)
        // initiate the cost matrix
        val start = System.currentTimeMillis()
        //      var computeSum = 0L
        //      var otherSum = 0L
        for {
          i <- 0 until jobSize
          j <- 0 until workerSize
        } {
          val initP1 = System.currentTimeMillis()
          val task = taskBuffer(i)
          val resource = resources(j)
          val initP2 = System.currentTimeMillis()
          //        otherSum += (initP2 - initP1)
          val cost = SchedulingUtils.calculateExecutionTime(task, resource, computeFactor, ioFactor, networkFactor)
          val initP3 = System.currentTimeMillis()
          //        computeSum += (initP3 - initP2)
          costMatrix(i)(j) = cost
          //        log.info(s" compute finished in ${initP3 - initP1} ms. input per task: ${task.inputs.length}, inputsClass:${task.inputs.getClass.getCanonicalName + " | " + task.inputSizes.getClass.getCanonicalName}")
        }
        //      val end = System.currentTimeMillis()
        System.arraycopy(costMatrix, 0, timeMatrix, 0, jobSize)
        //      log.info(s" initiation takes ${end -start} ms. computeSum: $computeSum ms, other sum: $otherSum ms. taskSize: $jobSize, workerSize: $workerSize, input per task: ${inputs(0).inputs.length}")
        val minminOpt = new MinCostOptimization(costMatrix, timeMatrix)
        val plan = minminOpt.execute()
        //      val end2 = System.currentTimeMillis()
        //      log.info(s" execution takes ${end2 - end} ms.")
        plan.foreach { tup =>
          results += (taskBuffer(tup._1).id -> resources(tup._2).toString)
          taskBuffer.remove(tup._1)
        }
//      }
    }
    results
  }

}
