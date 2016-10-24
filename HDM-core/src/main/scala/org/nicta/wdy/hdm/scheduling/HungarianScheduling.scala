package org.nicta.wdy.hdm.scheduling

import org.nicta.wdy.hdm.io.Path

import scala.collection.mutable

/**
 * Created by tiantian on 28/06/16.
 */
class HungarianScheduling extends SchedulingPolicy {


  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data, normally computeFactor >> ioFactor >= networkFactor
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return              pairs of planed tasks: (taskID -> workerPath)
   */
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): mutable.Map[String, Path] = {
    val taskBuffer = inputs.toBuffer
    val results = mutable.Map.empty[String, Path]

//    while(taskBuffer.nonEmpty){
      val jobSize = taskBuffer.length
      val workerSize = resources.length
      val costMatrix = Array.fill[Array[Double]](workerSize){
        Array.fill(jobSize){
          -1D
        }
      }
      // initiate the cost matrix
      for {
        i <- 0 until workerSize
        j <- 0 until jobSize
      } {
        val task = taskBuffer(j)
        val resource = resources(i)
        val cost = SchedulingUtils.calculateExecutionTime(task, resource, computeFactor, ioFactor, networkFactor)
        costMatrix(i)(j) = cost
      }
      val hungarianAlgo = new HungarianAlgorithm(costMatrix)
      val taskAssigned = hungarianAlgo.execute().filter(i => i != -1)
      val assigned = for(idx <- 0 until taskAssigned.length) yield {
        val resOffer = resources(idx)
        val taskIdx = taskAssigned(idx)
        val task = taskBuffer.apply(taskIdx)
        results += task.id -> resOffer
        task
      }
      taskBuffer --= assigned

//    }
    results
  }


}
