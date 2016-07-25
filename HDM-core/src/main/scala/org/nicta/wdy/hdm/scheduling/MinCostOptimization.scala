package org.nicta.wdy.hdm.scheduling

import java.util

import scala.collection.mutable

/**
 * Created by tiantian on 3/07/16.
 */

/**
 *
 * @param computationTimeMatrix   array[num of tasks][num of workers]
 * @param completionTimeMatrix    array[num of tasks][num of workers]
 */
class MinCostOptimization(var computationTimeMatrix:Array[Array[Double]], var completionTimeMatrix:Array[Array[Double]]) {
  var taskNum = computationTimeMatrix.length
  require(taskNum > 0)
  val workerNum = computationTimeMatrix(0).length
  require(workerNum > 0)
  var taskIdx = new Array[Int](computationTimeMatrix.length)
  var workerIdx = new Array[Int](computationTimeMatrix(0).length)
  var assignedWorkerNum = 0

  def init(): Unit = {
    //initiate idx vector
    var i = 0
    while(i < taskNum){
      taskIdx(i) = i
      i += 1
    }
    var j = 0
    while(j < workerNum){
      workerIdx(j) = 0
      j += 1
    }
  }


  def findNext(): (Int, Int) = {
    // each tuple represents (idx of the worker with min cost, cost) for each task
    val minTimeOnRes = new Array[(Int,Double)](taskNum)
    var i =0
    while (i < taskNum) {
      val costArray = completionTimeMatrix(i)
      if(costArray == null){
        println(s"i:$i, taskNum:$taskNum")
      }
      minTimeOnRes(i) = SchedulingUtils.minWithIndex(costArray)
      i += 1
    }
    //find the minimum value among minimum execution time vector
    val comp = (d1: (Int, Double), d2: (Int, Double)) => d1._2 < d2._2
    val minTimeOfTask = SchedulingUtils.minObjectsWithIndex(minTimeOnRes, comp)
    //(taskIdx, assigned worker)
    (minTimeOfTask._1, minTimeOfTask._2._1)
  }

  /**
   *
   *
   */
  def updateCostMatrix(taskIdx:Int, workerIdx:Int): Unit = {
    val cost = completionTimeMatrix(taskIdx)(workerIdx)
    var idx = 0
    while(idx < completionTimeMatrix.length){
      completionTimeMatrix(idx)(workerIdx) += cost
      idx += 1
    }
    computationTimeMatrix = removeElemFromArr(computationTimeMatrix, taskIdx)
    completionTimeMatrix= removeElemFromArr(completionTimeMatrix, taskIdx)

  }

  def execute(): Seq[(Int, Int)] ={
    val assignedResults = mutable.Buffer.empty[(Int, Int)]
    while(assignedWorkerNum < workerNum && taskNum > 0){
      val (tIdx, wIdx) = findNext()
      updateCostMatrix(tIdx, wIdx)
      assignedResults += ((tIdx, wIdx))
      if(workerIdx(wIdx) == 0) {
        assignedWorkerNum += 1
      }
      taskNum -= 1
      workerIdx(wIdx) += 1
    }
    assignedResults
  }


  def removeElemFromArr(arr:Array[Array[Double]], idx:Int): Array[Array[Double]] ={
    val updatedLen = arr.length - 1
    val updatedCMatrix = new Array[Array[Double]](updatedLen)
    if(idx > 0){
      System.arraycopy(arr, 0 , updatedCMatrix, 0, idx)
    }
    if(idx >= 0 && idx < updatedLen){
      System.arraycopy(arr, idx + 1 , updatedCMatrix, idx, updatedLen - idx)
    }
    updatedCMatrix
  }




}
