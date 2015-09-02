package org.nicta.wdy.hdm.scheduling

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable

/**
 * Created by tiantian on 2/09/15.
 */
class MinExecutionScheduling(val comparison:(Float, Float) => Boolean) extends SchedulingPolicy with Logging {

  val computationTimeMatrix = mutable.HashMap.empty[String, Vector[Float]]
  val jobCompletionTimeMatrix = mutable.HashMap.empty[String, Vector[Float]]


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
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Float, ioFactor: Float, networkFactor: Float): mutable.Map[String, String] = {
    val results = mutable.Map.empty[String, String]

    //initialize time matrices
    computationTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources, computeFactor, ioFactor, networkFactor)))
    jobCompletionTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources, computeFactor, ioFactor, networkFactor)))
    computationTimeMatrix foreach (println)
    jobCompletionTimeMatrix foreach (println)
    //find next scheduled job
    while (computationTimeMatrix.nonEmpty) {
      val selected = findNextMatching(jobCompletionTimeMatrix) // return (taskId, resourceIdx)
      val candidate = selected._1 -> resources.apply(selected._2).toString //transform to (taskId, resource)
      log.info(s"find next candidate: $candidate")
      results += candidate
      //update time matrices and remove selected jobs
      updateMatrix(computationTimeMatrix, jobCompletionTimeMatrix, selected)
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
  def calculateComputationTIme(p: SchedulingTask, resources: Seq[Path], computeFactor: Float, ioFactor: Float, networkFactor: Float): Vector[Float] = {
    resources.map { r =>
      SchedulingUtils.calculateExecutionTime(p, r, computeFactor, ioFactor, networkFactor)
      /*p.inputs.zip(p.inputSizes).map{tuple =>
        //compute completion time of each input partition for this task
        val dataLoadingTime = if(tuple._1.host == r.host) { // input is node local
          ioFactor * tuple._2
        } else { // normally networkFactor > ioFactor, which means loading remote data is slower than loading data locally
          networkFactor * tuple._2
        }
        val computeTime = computeFactor * tuple._2
        dataLoadingTime + computeTime
      }.sum // sum up the overall completion time for this task*/
    }.toVector
  }

  /**
   * find out the next minimum task
   * @param completionMatrix
   * @return (taskId, resourceIdx)
   */
  def findNextMatching(completionMatrix: mutable.HashMap[String, Vector[Float]]): (String, Int) = {

    //find the minimum execution time of each task
    val minTimeOnRes = completionMatrix.map { v =>
      v._1 -> SchedulingUtils.minWithIndex(v._2)
    }.toVector
    println("min execution time vector:")
    minTimeOnRes foreach (println(_))
    //find the minimum value among minimum execution time vector
    val comp = (d1: (String, (Int, Float)), d2: (String, (Int, Float))) => comparison(d1._2._2, d2._2._2)
    val minTimeOfTask = SchedulingUtils.minObjectsWithIndex(minTimeOnRes, comp)
    println(s"find min task:${minTimeOfTask._2} with expected execution time ${minTimeOfTask._2._2._2}")
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
  def updateMatrix(timeMatrix: mutable.HashMap[String, Vector[Float]], completionMatrix: mutable.HashMap[String, Vector[Float]], selected: (String, Int)): Unit = {
    val rIdx = selected._2
    val time = timeMatrix.get(selected._1).get.apply(rIdx)
    timeMatrix.remove(selected._1)
    completionMatrix.remove(selected._1)
    completionMatrix foreach { tup =>
      val oldValue = tup._2(rIdx)
      val newVector = tup._2.updated(rIdx, oldValue + time)
      completionMatrix.update(tup._1, newVector)
    }
  }

}

class MinMinScheduling extends MinExecutionScheduling(comparison = (d1:Float, d2:Float) => d1 < d2)

class MaxMinScheduling extends MinExecutionScheduling(comparison = (d1:Float, d2:Float) => d1 > d2)