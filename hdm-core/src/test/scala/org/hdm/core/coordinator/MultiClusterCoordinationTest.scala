package org.hdm.core.coordinator

import org.hdm.akka.server.SmsSystem
import org.hdm.core.message.AskCollaborateMsg
import org.junit.Test

/**
 * Created by tiantian on 10/05/16.
 */
class MultiClusterCoordinationTest {

  @Test
  def testAskCoordination(): Unit ={
    val master1 = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val master2 = "akka.tcp://masterSys@127.0.1.1:8998/user/smsMaster/ClusterExecutor"
    val msg = AskCollaborateMsg(master1)
    SmsSystem.forwardMsg(master2, msg) // ask master 2 to join master 1
    Thread.sleep(2000)
  }

}
