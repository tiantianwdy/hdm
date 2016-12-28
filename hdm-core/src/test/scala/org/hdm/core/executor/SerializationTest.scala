package org.hdm.core.executor

import org.hdm.akka.messages.Query
import org.hdm.akka.server.SmsSystem
import org.junit.Test
import org.hdm.core.executor.{HDMContext, AppContext}
import org.hdm.core.model.DDM

/**
 * Created by tiantian on 3/01/15.
 */
class SerializationTest extends ClusterTestSuite{



  @Test
  def sendDDMMsg(): Unit ={
    HDMContext.defaultHDMContext.init()
    val msg = DDM(Seq.empty[String], HDMContext.defaultHDMContext, new AppContext)
    //    val addmsg1 = AddMsg(CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.BlockManagerLeader", null)
    //    val res1 = SmsSystem.askMsg("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", addmsg1).getOrElse("no response")
    //    println(res1)

//    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster","org.hdm.core.coordinator.BlockManagerLeader", null)
//    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.hdm.core.coordinator.ClusterExecutorLeader", null)

    val res = SmsSystem.askSync("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", msg).getOrElse("no response")

    println(res)
  }
}
