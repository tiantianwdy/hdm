package org.nicta.wdy.hdm.scheduling

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.DataDependency
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable
import scala.reflect.ClassTag
/**
 * Created by tiantian on 23/06/15.
 */
trait SchedulingPolicy {

  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data, normally computeFactor >> ioFactor >= networkFactor
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return              pairs of planed tasks: (taskID -> workerPath)
   */
  def plan(inputs:Seq[SchedulingTask], resources:Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor:Double):mutable.Map[String, String]

}

case class SchedulingTask(id:String, inputs:Seq[Path], inputSizes:Seq[Long],  dep:DataDependency) {

  require(inputs.length == inputSizes.length)

}


class FairScheduling extends SchedulingPolicy with Logging{
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
    val results = mutable.Map.empty[String, String]
    val taskIter = inputs.iterator
    var idx = 0
    while(taskIter.hasNext){
      idx = (idx + 1) % resources.size
      val task = taskIter.next()
      val r = resources(idx)
      results += (task.id -> r.toString)
    }
    results
  }
}




object SchedulingUtils {


  def calculateExecutionTime(p:SchedulingTask, resources: Path, computeFactor: Double, ioFactor: Double, networkFactor:Double, computeDistance:Boolean = true): Double = {
    val inputs = p.inputs
    val amounts =  p.inputSizes
    require(inputs.length == amounts.length)
    val length = inputs.length
    var idx = 0
    var sum = 0D
    while(idx < length){
      val in = inputs(idx)
      val size = amounts(idx)
      val dataLoadingTime = if (in.address == resources.address) {
        // input is process local
        ioFactor * size
      } else if (in.host == resources.host) {
        // input is node local
        ioFactor * 2 * size
      } else {
        // normally networkFactor > ioFactor, which means loading remote data is slower than loading data locally
        val distance = if(computeDistance) Path.calculateDistance(in, resources) else 1
        (networkFactor * distance) * size
      }
      val computeTime = computeFactor * size
      sum += dataLoadingTime + computeTime
      idx += 1
    }
    sum
  }

  /**
   * return the min value and its index
   * @param vec
   * @return
   */
  def minObjectsWithIndex[T:ClassTag](vec:Vector[T],  compare:(T,T)=> Boolean):(Int, T) = {
    var minV = vec.head
    var minIdx = 0
    for (i  <- 1 until  vec.length) {
      if(compare(vec(i), minV)){
        minV = vec(i)
        minIdx = i
      }
    }
    (minIdx, minV)
  }

  def minObjectsWithIndex[T:ClassTag](vec:Array[T],  compare:(T,T)=> Boolean):(Int, T) = {
    var minV = vec(0)
    var minIdx = 0
    var i = 0
    while (i < vec.length) {
      if(compare(vec(i), minV)){
        minV = vec(i)
        minIdx = i
      }
      i += 1
    }
    (minIdx, minV)
  }


  def minWithIndex[T:ClassTag](vec:Vector[T])(implicit ordering: Ordering[T]):(Int, T) = {
    var minV = vec.head
    var minIdx = 0
    for (i  <- 1 until  vec.length) {
      if(ordering.lt(vec(i), minV)){
        minV = vec(i)
        minIdx = i
      }
    }
    (minIdx, minV)
  }

  def minWithIndex(vec:Array[Double]):(Int, Double) = {
    var minV = vec(0)
    var minIdx = 0
    var i = 0
    while (i < vec.length) {
      if(vec(i) < minV){
        minV = vec(i)
        minIdx = i
      }
      i += 1
    }
    (minIdx, minV)
  }
}