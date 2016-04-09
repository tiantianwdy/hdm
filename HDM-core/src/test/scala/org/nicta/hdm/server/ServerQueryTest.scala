package org.nicta.hdm.server

import com.baidu.bpit.akka.server.SmsSystem
import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.message._

/**
 * Created by tiantian on 8/04/16.
 */
class ServerQueryTest {

  val masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
  @Test
  def testAppListQuery (): Unit ={
    val msg = ApplicationsQuery
    val resp = SmsSystem.askSync(masterPath, msg)
    println(resp)
  }

  @Test
  def testAppInstanceQuery (): Unit ={
    val app = HDMContext.appName
    val version = HDMContext.version
    val msg = ApplicationInsQuery(app, version)
    val resp = SmsSystem.askSync(masterPath, msg)
    println(resp)
  }

  @Test
  def testLogicalFLow (): Unit ={
    val app = HDMContext.appName
    val version = HDMContext.version
    val msg1 = ApplicationInsQuery(app, version)
    val resp1 = SmsSystem.askSync(masterPath, msg1).get.asInstanceOf[ApplicationInsResp]
    println(resp1)
    val exeId = resp1.results.head
    val msg = LogicalFLowQuery(exeId, false)
    val resp2 = SmsSystem.askSync(masterPath, msg)
    println(resp2)
  }

  @Test
  def testGetExecutionDAG(): Unit ={
    val app = HDMContext.appName
    val version = HDMContext.version
    val msg1 = ApplicationInsQuery(app, version)
    val resp1 = SmsSystem.askSync(masterPath, msg1).get.asInstanceOf[ApplicationInsResp]
    println(resp1)
    val exeId = resp1.results.head
    val msg = ExecutionTraceQuery(exeId)
    val resp2 = SmsSystem.askSync(masterPath, msg)
    println(resp2)
  }

}
