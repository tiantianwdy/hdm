package org.hdm.core.server

import java.util.concurrent.{ExecutorService, CountDownLatch, Executors}

import org.hdm.core.scheduling.Scheduler
import org.hdm.core.server.{TreeResourceManager, DefResourceManager, ResourceManager, SingleClusterResourceManager}
import org.junit.{Test, Before}
import org.hdm.core.utils.Utils
import scala.util.Random

/**
 * Created by tiantian on 16/05/16.
 */
class ResourceManagerTest {

  val resManager = new DefResourceManager
  val resNum = 20
  val consumerNum = 240
  val executor = Executors.newFixedThreadPool(consumerNum * 2)

  class ResConsumer(resManager:ResourceManager, counter:CountDownLatch, executionService:ExecutorService) extends  Runnable {

    override def run(): Unit = {
      resManager.waitForNonEmpty()
      val candidates = Scheduler.getAllAvailableWorkers(resManager.getAllResources())
      val chosen = Utils.randomize(candidates).head.toString
      resManager.decResource(chosen, 1)
      println(s"get resource ${chosen} ...")
      executor.submit(new ResProducer(chosen, resManager, counter:CountDownLatch))
    }
  }

  class ResProducer(resId:String, resManager:ResourceManager, counter:CountDownLatch) extends  Runnable {

    override def run(): Unit = {
      val time = Random.nextFloat() * 10
      Thread.sleep(time.toLong)
      resManager.incResource(resId, 1)
      println(s"return resource ${resId} ...")
      counter.countDown()
      println(s"Current counting: ${counter.getCount}")
    }
  }

  @Before
  def init(): Unit = {

    for(idx <- 0 until resNum){
      val resId = s"resource-$idx"
      resManager.addResource(resId, 4)
    }
    println("===========Init resource ============")
    println(resManager.workingSize.availablePermits())
    println(resManager.getAllResources().toSeq.mkString("\n"))
  }

  @Test
  def testConcurrentAccess(): Unit ={
    val start = System.currentTimeMillis()
    val counter = new CountDownLatch(consumerNum)

    for(idx <- 0 until  consumerNum){
      executor.submit(new ResConsumer(resManager, counter, executor))
    }
    counter.await()
    val end = System.currentTimeMillis()
    println("===========After Test resource ============")
    println(resManager.workingSize.availablePermits())
    println(resManager.getAllResources().toSeq.mkString("\n"))
    println(s"TEst finished in ${end - start} ms")
  }

}
