package org.hdm.core.math

import org.hdm.core.context.{HDMServerContext, AppContext}
import org.hdm.core.server.HDMEngine
import org.junit.{After, Before}
import org.hdm.core.model.HDM

import scala.util.Random

/**
 * Created by tiantian on 1/12/16.
 */
class HDMathTestSuite {

  val numColumn = 100
  val numRows = 1000
  val vecData = Seq.fill[Double](numRows){
    Random.nextDouble()
  }

  val hdmContext = HDMServerContext.defaultContext
  hdmContext.clusterExecution.set(false)
//  val appContext = AppContext(masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
  AppContext.defaultAppContext.masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
  implicit val parallelism = 1
  implicit val hDMEntry = HDMEngine()

  @Before
  def beforeTest(): Unit ={
    hDMEntry.init() // local execution
    Thread.sleep(1000)
  }

  def printData(hdm: HDM[_]):Unit = {
    hdm.sample(20, 50000).foreach { elem =>
      println(elem)
    }
  }


  @After
  def afterTest(): Unit ={
    hDMEntry.shutdown(AppContext.defaultAppContext)
  }
}
