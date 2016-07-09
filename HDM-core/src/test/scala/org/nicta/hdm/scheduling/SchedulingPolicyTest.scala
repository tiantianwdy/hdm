package org.nicta.hdm.scheduling

import java.util.concurrent.LinkedBlockingQueue

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.OneToOne
import org.nicta.wdy.hdm.scheduling._

import scala.collection.mutable
import scala.util.Random
import scala.collection.JavaConversions._

/**
 * Created by tiantian on 1/09/15.
 */
class SchedulingPolicyTest extends SchedulingTestData {

  val numWorker = 8
  val pathPool = initAddressPool(numWorker)

  def generateInput(n:Int, sizeRange:Long):Seq[(Path, Long)] ={
    generateInputPath(pathPool, n).map(Path(_)) zip Seq.fill(n){(Random.nextDouble() * sizeRange).toLong + 1L}
  }

  def generateTasks(tNum:Int, pNum:Int):Seq[SchedulingTask] = {
    val queue = new LinkedBlockingQueue[SchedulingTask]
    var i=0
    while(i < tNum){
      val (input, inputSize) = generateInput(pNum, 102410241024L).unzip
      val task = SchedulingTask(HDMContext.newLocalId(), input, inputSize,  OneToOne)
      queue.add(task)
      i += 1
    }
//    Seq.fill(tNum) {
//      val (input, inputSize) = generateInput(pNum, 102410241024L).unzip
//      SchedulingTask(HDMContext.newLocalId(), input, inputSize,  OneToOne)
//    }
    queue.toSeq
  }

  def generateResources(amount:Int = 8):Seq[Path]= {
    val pathPool = initAddressPool(amount)
    generateWorkers(pathPool).map(Path(_))
  }

  def getSchedulingReport(schedulingPolicy:SchedulingPolicy, tasks:Seq[SchedulingTask], resources:Seq[Path], computeFactor: Float, ioFactor: Float, networkFactor:Float): Unit ={
    val taskMap = tasks.map(t => t.id -> t).toMap
    val startTime = System.currentTimeMillis()
    val comparedJobs = schedulingPolicy.plan(tasks, resources, computeFactor, ioFactor, networkFactor)
    val endTime = System.currentTimeMillis()
    println(s"time taken for scheduling: ${endTime - startTime} ms.")
    val exeTimeOfTasks = comparedJobs.groupBy(_._2).map { kSeq => // (resourceId, taskList)
      kSeq._1 -> kSeq._2.map { kv => // (taskId, resourceId)
        val t = taskMap.get(kv._1).get
        SchedulingUtils.calculateExecutionTime(t, Path(kSeq._1), computeFactor, ioFactor, networkFactor)
      }.sum //sum up total execution time on one resource
    }
    exeTimeOfTasks.foreach { time =>
      println(time)
    }
  }

  @Test
  def testMinminScheduling(): Unit ={
    val numTask = 4
    val inputEachTask = 2
    val tasks = generateTasks(numTask, inputEachTask)
    val resources = generateResources(2)
    val cpuFactor = 1F
    val ioFactor = 10F
    val networkFactor = 20F

    val schedulingPolicy = new MinMinScheduling
    val scheduledJobs = schedulingPolicy.plan(tasks, resources, 1F, 10F, 20F)
    scheduledJobs foreach(println(_))

  }

  @Test
  def testSchedulingComparison: Unit ={
    val numTask = 160
    val inputEachTask = 1067
    val tasks = generateTasks(numTask, inputEachTask)
    val resources = generateResources(160)

    val cpuFactor = 1F
    val ioFactor = 5F
    val networkFactor = 10F
//    println("================ min-min scheduling =======================")
//    val schedulingPolicy = new MinMinScheduling
//    getSchedulingReport(schedulingPolicy,tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ min-min opt scheduling =======================")
    val maxMinScheduling = new MinminSchedulingOpt
    getSchedulingReport(maxMinScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

    // compare with max-min scheduling
//    println("================ max-min scheduling =======================")
//    val maxMinScheduling = new MaxMinScheduling
//    getSchedulingReport(maxMinScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)
    // compare with fair-scheduling
//    println("================ fair-scheduling =======================")
//    getSchedulingReport(new FairScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

//    println("================ Simple-scheduling =======================")
//    getSchedulingReport(new OneByOneScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

//    println("================ Hungarian Scheduling =======================")
//    getSchedulingReport(new HungarianScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)
  }

}
