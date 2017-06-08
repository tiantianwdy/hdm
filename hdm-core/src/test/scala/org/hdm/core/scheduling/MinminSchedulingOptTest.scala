package org.hdm.core.scheduling

import org.junit.{Before, Test}
import org.hdm.core.scheduling.{MinCostOptimization, MinminSchedulingOpt}

/**
 * Created by Tiantian on 2016/7/25.
 */
class MinminSchedulingOptTest {

  val minminScheduler = new MinminSchedulingOpt



  val taskSize = 9

  val workerSize = 4

  val costMatrix = Array.fill[Array[Double]](taskSize) {
    Array.fill[Double](workerSize){
      0D
    }
  }

  def fillCostMatrix(){
    costMatrix(0) = Array(1D, 3D, 2D, 0.5D)
    costMatrix(1) = Array(0.4D, 1D, 2D, 1D)
    costMatrix(2) = Array(1D, 0.6D, 2D, 4D)
    costMatrix(3) = Array(3D, 1D, 0.5D, 2D)
    costMatrix(4) = Array(4D, 2D, 0.7D, 1D)
    costMatrix(5) = Array(3D, 1.9D, 2D, 0.6D)
    costMatrix(6) = Array(0.8D, 1.2D, 2.3D, 3D)
    costMatrix(7) = Array(1.2D, 3D, 2D, 3D)
    costMatrix(8) = Array(0.33D, 2.1D, 2.4D, 2.9D)
  }

  def printMatrix(matrix:Array[Array[Double]]){
    val rowN = matrix.length
    for(idx <- 0 until rowN){
      println(s"$idx: ${matrix(idx).mkString("["," , ", "]")}")
    }
  }

  @Before
  def preTest(){
    fillCostMatrix()
    println("============== init matrix ==============")
    printMatrix(costMatrix)
  }

  @Test
  def testMinminOpt(){
    var waitingTaskSize = taskSize
    var jctMatrix = new Array[Array[Double]](waitingTaskSize)
    Array.copy(costMatrix, 0, jctMatrix, 0, waitingTaskSize)
    val minminOpt = new MinCostOptimization(costMatrix, jctMatrix)
    minminOpt.init()
    while(minminOpt.completionTimeMatrix.length > 0 ){
      val next = minminOpt.findNext()
      println(next)
      minminOpt.updateCostMatrix(next._1, next._2)
      minminOpt.taskNum -= 1
      if(minminOpt.taskNum > 0){
        println(s"task number remaining:${minminOpt.taskNum}")
        printMatrix(minminOpt.completionTimeMatrix)
      }
    }

  }

}
