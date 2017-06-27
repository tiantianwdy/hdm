package org.hdm.core.executor

import org.hdm.akka.messages.AddMsg
import org.hdm.akka.server.SmsSystem
import org.hdm.core.model.DDM
import org.junit.{Ignore, After, Before, Test}

/**
 * Created by tiantian on 3/01/15.
 */
class SerializationTest extends ClusterTestSuite {


  @Before
  def beforeTest(): Unit ={
    hDMContext.clusterExecution.set(false)
    hDMEntry.init()
    appContext.setMasterPath("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(1000)
  }

  @Ignore("old tests, to be released")
  @Test
  def sendDDMMsg(): Unit ={
    val msg = DDM(Seq.empty[String], hDMContext, appContext)

    val res = SmsSystem.askSync("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster", msg).getOrElse("no response")

    println(res)
  }

  @After
  def afterTest(): Unit ={
    hDMEntry.shutdown(appContext)
  }
}
