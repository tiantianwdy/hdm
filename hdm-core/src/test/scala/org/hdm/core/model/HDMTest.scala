package org.hdm.core.model

import org.hdm.core.context.HDMContext
import org.hdm.core.executor.ClusterTestSuite
import org.hdm.core.server.HDMServerContext
import org.junit.{Before, Test}

import scala.util.Random

/**
 * Created by tiantian on 30/11/16.
 */
class HDMTest extends ClusterTestSuite {


  val data = Seq.fill[Double](10000){
    Random.nextDouble()
  }


  implicit val parallelism = 1

  @Before
  def beforeTest(): Unit ={
    appContext.setMasterPath("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    hDMContext.clusterExecution.set(false)
    hDMContext.init()
    Thread.sleep(1000)
  }

  @Test
  def testParallelize(): Unit ={
    import HDMContext._
    val input = HDM.parallelize(elems = data, hdmContext = hDMContext,  appContext = appContext, numOfPartitions = 20)
    println( input.children.size)
    val res = input.map(d => (d, 1)).mapValues(_ + 1)
//    hdmContext.explain(res, parallelism).logicalPlanOpt
    .sample(20, 50000)
    .foreach { elem =>
      println(elem)
    }

    Thread.sleep(5000)
  }


  @Test
  def afterTest(): Unit ={
    hDMContext.shutdown(appContext)
  }
}
