package org.nicta.wdy.hdm.planing

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
   * @param loadFactor
   * @param computeFactor
   * @return
   */
  def plan(inputs:Seq[Partition], resources:Seq[String], loadFactor:Float, computeFactor:Float):mutable.Map[String, Seq[String]]

}


class MinMinScheduling extends SchedulingPolicy{
  /**
   *
   * @param inputs
   * @param resources
   * @param loadFactor
   * @param computeFactor
   * @return
   */
  override def plan(inputs: Seq[Partition], resources: Seq[String], loadFactor: Float, computeFactor: Float): mutable.Map[String, Seq[String]] = {
    val results = mutable.Map.empty[String, Seq[String]]
    val computationTimeMatrix = mutable.HashMap.empty[String, Vector[Float]]
    val jobCompletionTimeMatrix = mutable.HashMap.empty[String, Vector[Float]]
    computationTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources,loadFactor, computeFactor)))
    jobCompletionTimeMatrix ++= inputs.map(p => (p.id -> calculateComputationTIme(p, resources, loadFactor, computeFactor)))
    val candidate = findNextMatching(jobCompletionTimeMatrix) // return (taskId, resourceId)
    results.getOrElseUpdate(candidate._1, mutable.Buffer.empty[String]) += candidate._2
    val selected = (candidate._1, resources.indexOf(candidate._2))// return (taskId, resource index)
    updateMatrix(computationTimeMatrix, jobCompletionTimeMatrix, selected)
    results
  }


  /**
   * compute the expected computation time of a partition on a set of given resources
   * @param p
   * @param resources
   * @param loadFactor
   * @param computeFactor
   * @return
   */
  def calculateComputationTIme(p:Partition, resources: Seq[String], loadFactor: Float, computeFactor: Float):Vector[Float] = {
    resources.map{r =>
      val loadTime = if(p.locationIdx == r) p.size else loadFactor * p.size
      val computeTime = computeFactor * p.size
      loadTime + computeTime
    }.toVector
  }

  /**
   * find out the next minimum task
   * @param completionMatrix
   * @return
   */
  def findNextMatching(completionMatrix:mutable.HashMap[String, Vector[Float]]):(String,String) ={
    import scala.collection.JavaConversions._
    val minTimeOnRes =  completionMatrix.map { v =>
      v._1 -> VectorUtils.minWithIndex(v._2)
    }.toVector
    val comp = (d1:(String,(String,Float)), d2:(String,(String,Float))) => d1._2._2 < d2._2._2
    val minTimeOfTask = VectorUtils.minObjectsWithIndex(minTimeOnRes, comp)
    val idx = minTimeOfTask._1
    // (taskId, resourceId)
    (minTimeOnRes(idx)._1, minTimeOfTask._2._1)
  }

  def updateMatrix(timeMatrix: mutable.HashMap[String, Vector[Float]], completionMatrix:mutable.HashMap[String, Vector[Float]], selected: (String, Int)):Unit ={
    val rIdx = selected._2
    val time = timeMatrix.get(selected._1).get.apply(rIdx)
    timeMatrix.remove(selected._1)
    completionMatrix.remove(selected._1)
    completionMatrix foreach{tup =>
      val oldValue = tup._2(rIdx)
      tup._2.updated(rIdx, oldValue + time)
    }
  }


}

case class Partition(id:String, size:Int, locationIdx:Int)

object VectorUtils {

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
}