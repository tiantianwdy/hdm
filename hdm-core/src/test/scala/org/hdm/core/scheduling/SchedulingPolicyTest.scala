package org.hdm.core.scheduling

import java.util.concurrent.{CopyOnWriteArrayList, LinkedBlockingQueue}

import org.junit.Test
import org.hdm.core.executor.HDMContext
import org.hdm.core.io.Path
import org.hdm.core.model.OneToOne
import org.hdm.core.scheduling._

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by tiantian on 1/09/15.
 */
class SchedulingPolicyTest extends SchedulingTestData {

  val numWorker = 8
  val defaultPathPool = initAddressPool(numWorker)

  def generateInput(pathPool:Seq[String], n:Int, sizeRange:Long):Seq[(Path, Long)] ={
    generateInputPath(pathPool, n).map(Path(_)) zip Seq.fill(n){(Random.nextDouble() * (sizeRange/2)).toLong + sizeRange/2}
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


  def calculateDataLocalityRate(assignedTasks:Map[Path, mutable.Map[String, Path]], tasks:Seq[SchedulingTask]): Double ={
    val taskMap = tasks.map(t => t.id -> t).toMap
    val dataLocalityIter = assignedTasks.map { kSeq =>
      val host = kSeq._1.address
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



  def getSchedulingReport(schedulingPolicy:SchedulingPolicy,
                          tasks:Seq[SchedulingTask],
                          resources:Seq[Path],
                          computeFactor: Float,
                          ioFactor: Float,
                          networkFactor:Float): Unit ={
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
        SchedulingUtils.calculateExecutionTime(t, kSeq._1, computeFactor, ioFactor, networkFactor)
      }.sum //sum up total execution time on one resource
    }
    val dataLocalityRate = calculateDataLocalityRate(groupedTasks, tasks)
//    exeTimeOfTasks.foreach { time =>
//      println(time)
//    }
    println(s"Data locality Rates: ${dataLocalityRate * 100} %.")
  }

  def mockSchedulingTest(schedulingPolicy:SchedulingPolicy,
                         tasks:Seq[SchedulingTask],
                         resources:Seq[Path],
                         computeFactor: Float,
                         ioFactor: Float,
                         networkFactor:Float) {

    implicit def executionContext:ExecutionContext = ExecutionContext.global

    val resRWLock = new ReentrantReadWriteLock()

    val taskMap = tasks.map(t => t.id -> t).toMap
    val taskBuffer = tasks.toBuffer
    val resourceBuffer = new CopyOnWriteArrayList[Path]()// needs to be type safe as multi-threading in collecting resources
    resourceBuffer ++=  resources
    var totalSchedulingTime = 0L
    val scheduledTasks = mutable.Map.empty[String, Path]

    while(taskBuffer.nonEmpty){
      if(resourceBuffer.nonEmpty) {
        resRWLock.readLock().lock()
        val startTime = System.currentTimeMillis()
        val comparedJobs = schedulingPolicy.plan(taskBuffer, resourceBuffer, computeFactor, ioFactor, networkFactor)
        val endTime = System.currentTimeMillis()
        resRWLock.readLock().unlock()
        totalSchedulingTime += endTime - startTime
//        println(s"time taken for scheduling: ${endTime - startTime} ms.")
        scheduledTasks ++= comparedJobs
        val assignedTasks = comparedJobs.toSeq.map(kv => taskMap.get(kv._1).get)
        val assignedRes = comparedJobs.toSeq.map(_._2)
        taskBuffer --= assignedTasks
//        println(s"taskBuffer size: ${taskBuffer.size} ")

        val assignedPath = mutable.Buffer.empty[Path]
        resRWLock.readLock().lock()
        assignedRes.foreach {
          res =>
            resourceBuffer.find(_.toString == res) match {
              case Some(path) => assignedPath += path
              case None =>
            }
        }
        resRWLock.readLock().unlock()
        resRWLock.writeLock().lock()
        resourceBuffer --= assignedPath
        resRWLock.writeLock().unlock()
        if (taskBuffer.nonEmpty) {
          //randomly collect assignedRes in a size related order in future
          assignedPath.foreach {
            res =>
              Future {
                Thread.sleep(50 * Random.nextInt(5))
                resRWLock.writeLock().lock()
                resourceBuffer.add(res)
                resRWLock.writeLock().unlock()
              }
          }
        }
      } else {
        Thread.sleep(100)
      }

    }
    val groupedTasks = scheduledTasks.groupBy(_._2)
    val dataLocalityRate = calculateDataLocalityRate(groupedTasks, tasks)
    println(s"Assigned tasks:${scheduledTasks.size}, Data locality Rates: ${dataLocalityRate * 100} %.")
    println(s"Total time spent on scheduling: ${totalSchedulingTime} ms.")

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
          Path("akka.tcp://masterSys@127.10.1.1:8999/user/") -> mutable.Map("0" -> Path(""), "1" -> Path("")),
          Path("akka.tcp://masterSys@127.20.1.2:8999/user/") -> mutable.Map("2" -> Path(""), "3" -> Path(""))
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
    val numTask = 6000
    val inputEachTask = 1
    val hostNum = 20
    val workerPerHost = 8
    val resources = generateResourcesMultiCore(hostNum, workerPerHost)
    val resStr = resources.map(_.toString)
    val tasks = generateTasks(resStr, numTask, inputEachTask)
//    val resources = generateResources(160)

    val cpuFactor = 1F
    val ioFactor = 5F
    val networkFactor = 15F
//    println("================ min-min scheduling =======================")
//    val schedulingPolicy = new MinMinScheduling
//    getSchedulingReport(schedulingPolicy,tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ min-min opt scheduling =======================")
    val minminOpt = new MinminSchedulingOpt
    mockSchedulingTest(minminOpt, tasks, resources, cpuFactor, ioFactor, networkFactor)

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
    mockSchedulingTest(new HungarianScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)

    println("================ delay scheduling =======================")
    val delayScheduling = new DelayScheduling(0, 1, 256 << 8)
    mockSchedulingTest(delayScheduling, tasks, resources, cpuFactor, ioFactor, networkFactor)
  }

}
