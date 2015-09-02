package org.nicta.hdm.scheduling

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.OneToOne
import org.nicta.wdy.hdm.scheduling._

import scala.collection.mutable
import scala.util.Random

/**
 * Created by tiantian on 1/09/15.
 */
class SchedulingPolicyTest extends SchedulingTestData {

  val numWorker = 5
  val pathPool = initAddressPool(numWorker)

  def generateInput(n:Int, sizeRange:Int):Seq[(Path, Int)] ={
    generateInputPath(pathPool, n).map(Path(_)) zip Seq.fill(n){Random.nextInt(sizeRange) + 1}
  }

  def generateTasks(tNum:Int, pNum:Int):Seq[SchedulingTask] = {
    Seq.fill(tNum) {
      val (input, inputSize) = generateInput(pNum, 1).unzip
      SchedulingTask(HDMContext.newLocalId(), input, inputSize, 0, OneToOne)
    }
  }

  def generateResources():Seq[Path]= {
    generateWorkers(pathPool).map(Path(_))
  }

  def testFairSchedulingReport(tasks:Seq[SchedulingTask], resources:Seq[Path], computeFactor: Float, ioFactor: Float, networkFactor:Float): Unit ={
    val taskMap = tasks.map(t => t.id -> t).toMap
    val fairScheduling = new FairScheduling
    val comparedJobs = fairScheduling.plan(tasks, resources, computeFactor, ioFactor, networkFactor)
    val exeTimeofTasks = comparedJobs.groupBy(_._2).map { kSeq => // (resourceId, taskList)
      kSeq._1 -> kSeq._2.map { kv => // (taskId, resourceId)
        val t = taskMap.get(kv._1).get
        SchedulingUtils.calculateExecutionTime(t, Path(kSeq._1), computeFactor, ioFactor, networkFactor)
      }.sum //sum up total execution time on one resource
    }
    exeTimeofTasks.foreach { time =>
      println(time)
    }
  }

  @Test
  def testMinminScheduling(): Unit ={
    val numTask = 20
    val inputEachTask = 5
    val schedulingPolicy = new MinMinScheduling
    val tasks = generateTasks(numTask, inputEachTask)
    val resources = generateResources()
    val scheduledJobs = schedulingPolicy.plan(tasks, resources, 1F, 10F, 20F)
//    scheduledJobs foreach(println(_))

    // compare with max-min scheduling
    println("================ max-min scheduling =======================")
    val maxMinScheduling = new MaxMinScheduling
    maxMinScheduling.plan(tasks, resources, 1F, 10F, 20F)
    // compare with fair-scheduling
    println("================ fair-scheduling =======================")
    testFairSchedulingReport(tasks, resources, 1F, 10F, 20F)

  }


}
