package org.hdm.core.model


import org.junit.Test
import org.hdm.core.executor.{AppContext, ClusterExecutor, HDMContext}
import org.hdm.core.model.HDM

import scala.util.Random

/**
 * Created by tiantian on 30/11/16.
 */
class HDMTest {


  val data = Seq.fill[Double](10000){
    Random.nextDouble()
  }

  val hdmContext = HDMContext.defaultHDMContext
  AppContext.defaultAppContext.masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
  implicit val parallelism = 1

  @Test
  def testParallelize(): Unit ={
    import HDMContext._
    hdmContext.init()
    Thread.sleep(1000)

    val input = HDM.parallelize(elems = data, numOfPartitions = 20)
    println( input.children.size)
    val res = input.map(d => (d, 1)).mapValues(_ + 1)
//    hdmContext.explain(res, parallelism).logicalPlanOpt
    .sample(20, 50000)
    .foreach { elem =>
      println(elem)
    }

    Thread.sleep(5000)
  }

}
