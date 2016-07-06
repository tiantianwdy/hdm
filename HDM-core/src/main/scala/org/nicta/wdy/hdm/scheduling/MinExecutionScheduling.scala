package org.nicta.wdy.hdm.scheduling

import java.util

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable

/**
 * Created by tiantian on 2/09/15.
 */
class MinExecutionScheduling(val comparison:(Double, Double) => Boolean) extends SchedulingPolicy with Logging {

  val computationTimeMatrix = new mutable.HashMap[String, Array[Double]]()
  val jobCompletionTimeMatrix = new mutable.HashMap[String, Array[Double]]


  def init(): Unit = {
    computationTimeMatrix.clear()
    jobCompletionTimeMatrix.clear()
  }

  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return
   */
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): mutable.Map[String, String] = {
    require(resources != null && resources.nonEmpty)

    val results = mutable.Map.empty[String, String]
//    initialize time matrices
    val start = System.currentTimeMillis()
    inputs foreach { task =>
      val cost = calculateComputationTIme(task, resources, computeFactor, ioFactor, networkFactor)
      val costCopy = util.Arrays.copyOf(cost, cost.length)
      computationTimeMatrix += (task.id -> cost)
      jobCompletionTimeMatrix += (task.id -> costCopy)
    }
//    computationTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources, computeFactor, ioFactor, networkFactor)))
//    jobCompletionTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources, computeFactor, ioFactor, networkFactor)))
//    val time1 = System.currentTimeMillis()
//    log.info(s"initiate matrix in ${time1 - start} ms.")

    //find next scheduled job
    while (!computationTimeMatrix.isEmpty) { //todo: revise to only schedule one task for each worker every time rather than schedule all tasks.
//      val time2 = System.currentTimeMillis()
      val selected = findNextMatching(jobCompletionTimeMatrix) // return (taskId, resourceIdx)
//      val time3 = System.currentTimeMillis()
      val candidate = selected._1 -> resources.apply(selected._2).toString //transform to (taskId, resource)
//      log.debug(s"find next candidate: $candidate in ${time3 - time2} ms.")
      results += candidate
//    update time matrices and remove selected jobs
      updateMatrix(computationTimeMatrix, jobCompletionTimeMatrix, selected)
//    val time4 = System.currentTimeMillis()
//    log.debug(s"update matrix in ${time4 - time3} ms.")
    }
    results
  }


  /**
   * compute the expected computation time of a partition on a set of given resources
   * @param p
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return
   */
  def calculateComputationTIme(p: SchedulingTask, resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): Array[Double] = {
//    resources.map { r =>
//      SchedulingUtils.calculateExecutionTime(p, r, computeFactor, ioFactor, networkFactor)
//    }.toVector
    val costArray = new Array[Double](resources.length)
    var i = 0
    while(i < resources.length){
      costArray(i) = SchedulingUtils.calculateExecutionTime(p, resources(i), computeFactor, ioFactor, networkFactor)
      i += 1
    }
    costArray
  }

  /**
   * find out the next minimum task
   * @param completionMatrix
   * @return (taskId, resourceIdx)
   */
  def findNextMatching(completionMatrix: mutable.HashMap[String, Array[Double]]): (String, Int) = {

    //find the minimum execution time of each task
//    val minTimeOnRes = completionMatrix.map { v =>
//      v._1 -> SchedulingUtils.minWithIndex(v._2)
//    }.toVector
    val minTimeOnRes = new Array[(String,(Int,Double))](completionMatrix.size)
    var i = 0
    for (costArray <- completionMatrix) {
      minTimeOnRes(i) = (costArray._1, SchedulingUtils.minWithIndex(costArray._2))
      i += 1
    }
//    minTimeOnRes foreach (println(_))
    //find the minimum value among minimum execution time vector
    val comp = (d1: (String, (Int, Double)), d2: (String, (Int, Double))) => comparison(d1._2._2, d2._2._2)
    val minTimeOfTask = SchedulingUtils.minObjectsWithIndex(minTimeOnRes, comp)
//    log.trace(s"find min task:${minTimeOfTask._2} with expected execution time ${minTimeOfTask._2._2._2}, in ${end2 - end} ms.")
    val idx = minTimeOfTask._1
    // (taskId, resourceIdx)
    (minTimeOnRes(idx)._1, minTimeOfTask._2._2._1)
  }

  /**
   * remove selected task from computation time matrix,
   * update expected completion time by adding the time offset of selected task.
   *
   * @param timeMatrix absolute execution time for each task on a set of resources
   * @param completionMatrix the current estimated completion time matrix for all the tasks on given set of resources
   * @param selected selected task with index
   *
   */
  def updateMatrix(timeMatrix: mutable.HashMap[String, Array[Double]], completionMatrix:mutable.HashMap[String, Array[Double]], selected: (String, Int)): Unit = {
    val rIdx = selected._2
    val time = timeMatrix.get(selected._1).get.apply(rIdx)
    timeMatrix.remove(selected._1)
    completionMatrix.remove(selected._1)
    completionMatrix foreach { tup =>
      val oldValue = tup._2(rIdx)
      tup._2(rIdx) = oldValue + time
//      val newVector = tup._2.updated(rIdx, oldValue + time)
//      completionMatrix.update(tup._1, newVector)
    }
  }

}

class MinMinScheduling extends MinExecutionScheduling(comparison = (d1:Double, d2:Double) => d1 < d2)

class MaxMinScheduling extends MinExecutionScheduling(comparison = (d1:Double, d2:Double) => d1 > d2)