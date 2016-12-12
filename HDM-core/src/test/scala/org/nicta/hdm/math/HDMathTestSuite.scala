package org.nicta.hdm.math

import org.junit.Before
import org.nicta.wdy.hdm.executor.{AppContext, HDMContext}
import org.nicta.wdy.hdm.model.HDM

import scala.util.Random

/**
 * Created by tiantian on 1/12/16.
 */
class HDMathTestSuite {

  val data = Seq.fill[Double](1000){
    Random.nextDouble()
  }

  val hdmContext = HDMContext.defaultHDMContext
//  val appContext = AppContext(masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
  AppContext.defaultAppContext.masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
  implicit val parallelism = 1

  @Before
  def beforeTest(): Unit ={
    hdmContext.init() // local execution
    Thread.sleep(1000)
  }

  def printData(hdm: HDM[_]):Unit = {
    hdm.sample(20, 50000).foreach { elem =>
      println(elem)
    }
  }

}
