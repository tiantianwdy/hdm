package org.nicta.hdm.server

import org.hdm.akka.configuration.ActorConfig
import org.hdm.akka.messages.{Reply, Query}
import org.hdm.akka.server.SmsSystem
import org.junit.Test
import org.nicta.wdy.hdm.executor.{AppContext, HDMContext}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.message._

/**
 * Created by tiantian on 8/04/16.
 */
class ServerQueryTest {

  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  val masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"

  @Test
  def testAppListQuery (): Unit ={
    val msg = ApplicationsQuery
    val resp = SmsSystem.askSync(masterPath, msg)
    println(resp)
  }

  @Test
  def testAppVersionsQuery (): Unit ={
    val msg = new AllAppVersionsQuery()
    val resp = SmsSystem.askSync(masterPath, msg)
    println(resp)
  }

  @Test
  def testAppInstanceQuery (): Unit ={
    val app = appContext.appName
    val version = appContext.version
    val msg = ApplicationInsQuery(app, version)
    val resp = SmsSystem.askSync(masterPath, msg)
    println(resp)
  }

  @Test
  def testLogicalFLow (): Unit ={
    val app = appContext.appName
    val version = appContext.version
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
    val app = appContext.appName
    val version = appContext.version
    val msg1 = ApplicationInsQuery(app, version)
    val resp1 = SmsSystem.askSync(masterPath, msg1).get.asInstanceOf[ApplicationInsResp]
    println(resp1)
    val exeId = resp1.results.head
    val msg = ExecutionTraceQuery(exeId)
    val resp2 = SmsSystem.askSync(masterPath, msg)
    println(resp2)
  }

  @Test
  def testGetAllSlaves(): Unit ={
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/"
    val msg1 = Query("smsSystem/allSlaves", "", "")
    val resp1 = SmsSystem.askSync(master, msg1) match {
      case Some(res) => res match {
        case Reply(result, _, _, _, _, _) => result match {
          case lst: List[ActorConfig] => lst foreach( act => println(Path(act.actorPath).address))
        }
        case other =>
      }
      case other =>
    }
//    val msg2 = Query("dataService/dataLike", "system/cpu", "127.0.1.1:20010/cpuRate")
//    val resp2 = SmsSystem.askSync(master, msg2).get.asInstanceOf[Reply]
//    println(resp2.result.asInstanceOf[Array[Array[_]]].map(_.mkString("(", ",", ")")).mkString("\n"))
  }

  @Test
  def testGetMonitorData(): Unit ={
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/"
    val prop = "system/jvm"
    val node = "127.0.1.1:20010"
    val msg1 = Query("dataService/keys", prop, "127.0.1.1")
    val resp1 = SmsSystem.askSync(master, msg1).get.asInstanceOf[Reply]
    resp1.result.asInstanceOf[Array[_]] foreach { key =>
      println(key)
      val msg2 = Query("dataService/getData", prop, key.toString)
      val resp2 = SmsSystem.askSync(master, msg2).get.asInstanceOf[Reply]
      println(resp2.result.asInstanceOf[Array[Array[_]]].map(_.mkString("(", ",", ")")).mkString("\n"))
    }
  }

}
