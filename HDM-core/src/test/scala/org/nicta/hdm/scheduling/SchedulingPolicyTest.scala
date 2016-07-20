package org.nicta.hdm.scheduling

import java.util.concurrent.LinkedBlockingQueue

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.OneToOne
import org.nicta.wdy.hdm.scheduling._

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by tiantian on 1/09/15.
 */
class SchedulingPolicyTest extends SchedulingTestData {

  val numWorker = 8
  val defaultPathPool = initAddressPool(numWorker)

  def generateInput(pathPool:Seq[String], n:Int, sizeRange:Long):Seq[(Path, Long)] ={
    generateInputPath(pathPool, n).map(Path(_)) zip Seq.fill(n){(Random.nextDouble() * sizeRange).toLong + 1L}
  }

  def generateTasks(pathPool:Seq[String], tNum:Int, pNum:Int):Seq[SchedulingTask] = {
    val queue = new LinkedBlockingQueue[SchedulingTask]
    var i=0
    while(i < tNum){
      val (input, inputSize) = generateInput(pathPool, pNum, 102410241024L).unzip
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

  def generateResourcesMultiCore(hostNum:Int = 20, workerPerHost:Int = 8):Seq[Path] = {
    val pathPool = initAddressPool(hostNum)
    generateWorkerPath(pathPool, workerPerHost)
  }


  def calculateDataLocalityRate(assignedTasks:Map[String, mutable.Map[String, String]], tasks:Seq[SchedulingTask]): Double ={
    val taskMap = tasks.map(t => t.id -> t).toMap
    val dataLocalityIter = assignedTasks.map { kSeq =>
      val host = Path(kSeq._1).address
      val tasks = kSeq._2.map { kv =>
        taskMap.get(kv._1).get
      }
      var totalSize = 0L
      var localSize = 0L
      tasks.foreach{ t =>
        val tup = t.inputs.zip(t.inputSizes)
        tup.foreach { kv =>
          if(kv._1.address == host) localSize += kv._2
          totalSize += kv._2
        }
      }
      (localSize, totalSize)
    }

    val dataLocalityRate = dataLocalityIter.reduceLeft{ (tup1, tup2) =>
      (tup1._1 + tup2._1, tup1._2 + tup2._2)
    }
    dataLocalityRate._1.toDouble / dataLocalityRate._2
  }



  def getSchedulingReport(schedulingPolicy:SchedulingPolicy, tasks:Seq[SchedulingTask], resources:Seq[Path], computeFactor: Float, ioFactor: Float, networkFactor:Float): Unit ={
    val taskMap = tasks.map(t => t.id -> t).toMap
    val startTime = System.currentTimeMillis()
    val comparedJobs = schedulingPolicy.plan(tasks, resources, computeFactor, ioFactor, networkFactor)
    val endTime = System.currentTimeMillis()
    println(s"time taken for scheduling: ${endTime - startTime} ms.")
    val groupedTasks = comparedJobs.groupBy(_._2)
//    val assignedTasks = groupedTasks.map(kMap => kMap._1 -> kMap._2.map(_._1))
    val exeTimeOfTasks = groupedTasks.map { kSeq => // (resourceId, taskList)
      kSeq._1 -> kSeq._2.map { kv => // (taskId, resourceId)
        val t = taskMap.get(kv._1).get
        SchedulingUtils.calculateExecutionTime(t, Path(kSeq._1), computeFactor, ioFactor, networkFactor)
      }.sum //sum up total execution time on one resource
    }
    val dataLocalityRate = calculateDataLocalityRate(groupedTasks, tasks)
//    exeTimeOfTasks.foreach { time =>
//      println(time)
//    }
    println(s"Data locality Rates: ${dataLocalityRate * 100} %.")
  }

  @Test
  def testCalculateDataLocalityRate(): Unit ={
    val inputs = Seq (
      (Path("akka.tcp://masterSys@127.10.1.1:8999/user/smsMaster/"), 4),
      (Path("akka.tcp://masterSys@127.10.10.1:8999/user/smsMaster/"), 2),
      (Path("akka.tcp://masterSys@127.20.1.2:8999/user/smsMaster/"), 2),
      (Path("akka.tcp://masterSys@127.20.9.1:8999/user/smsMaster/"), 2)
    )
    val tasks = inputs.map { kv =>
      val idx = inputs.indexOf(kv)
      SchedulingTask(idx.toString, Seq(kv._1), Seq(kv._2),  OneToOne)
    }
    val assigns = Map(
          "akka.tcp://masterSys@127.10.1.1:8999/user/" -> mutable.Map("0" -> "", "1" -> ""),
          "akka.tcp://masterSys@127.20.1.2:8999/user/" -> mutable.Map("2" -> "", "3" -> "")
    )
    println(calculateDataLocalityRate(assigns, tasks))
  }

  @Test
  def testMinminScheduling(): Unit ={
    val numTask = 4
    val inputEachTask = 2
    val tasks = generateTasks(defaultPathPool, numTask, inputEachTask)
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
    val numTask = 1067
    val inputEachTask = 1
    val hostNum = 20
    val workerPerHost = 8
    val resources = generateResourcesMultiCore(hostNum, workerPerHost)
    val resStr = resources.map(_.toString)
    val tasks = generateTasks(resStr, numTask, inputEachTask)
//    val resources = generateResources(160)

    val cpuFactor = 1F
    val ioFactor = 5F
    val networkFactor = 10F
//    println("================ min-min scheduling =======================")
//    val schedulingPolicy = new MinMinScheduling
//    getSchedulingReport(schedulingPolicy,tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ min-min opt scheduling =======================")
    val minminOpt = new MinminSchedulingOpt
    getSchedulingReport(minminOpt, tasks, resources, cpuFactor, ioFactor, networkFactor)

//    compare with max-min scheduling
//    println("================ max-min scheduling =======================")
//    val maxMinScheduling = new MaxMinScheduling
//    getSchedulingReport(maxMinScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)
//    compare with fair-scheduling
//    println("================ fair-scheduling =======================")
//    getSchedulingReport(new FairScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

//    println("================ Simple-scheduling =======================")
//    getSchedulingReport(new OneByOneScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ Hungarian Scheduling =======================")
    getSchedulingReport(new HungarianScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ delay scheduling =======================")
    val delayScheduling = new DelayScheduling(0, 1, 256 << 8)
    getSchedulingReport(delayScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)
  }

}
