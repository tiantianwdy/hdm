package com.baidu.bpit.akka.server

import junit.framework.TestCase
import akka.actor.Props
import com.baidu.bpit.akka.monitor.TestMonitor
import akka.actor.AddressFromURIString
import akka.actor.Deploy
import akka.remote.RemoteScope
import com.baidu.bpit.akka.messages.AddMsg
import com.baidu.bpit.akka.configuration.Parameters
import com.baidu.bpit.akka.TestConfig
import org.junit.After
import com.baidu.bpit.akka.messages.CollectMsg
import com.sun.org.apache.xalan.internal.xsltc.runtime.Parameter
import com.baidu.bpit.akka.messages.InitMsg

class SmsSystemTest extends TestCase with TestConfig {

  def testStartSmsSystem() {
    SmsSystem.startAsMaster(8998, false)
    //later starting up will cover the previous one
//    SmsSystem.startAsSlave("akka://smsSystem@172.22.218.167:15100", 15010, false)
    SmsSystem.addActor("testActor", "localhost", "com.baidu.bpit.akka.MyClusterActor", null)
    Thread.sleep(10000)
  }

  def testRegisterMonitor() {
    new Thread {
      override def run {
        SmsSystem.startAsMaster(8999, false, testMasterConf)
      }
    }.start
    Thread.sleep(2000)
    SmsSystem.startAsSlave("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", 10010, false, testSlaveConf)
    val instance1 = SmsSystem.system.actorOf(Props(new TestMonitor("instance_1", 15007)))
    val instance2 = SmsSystem.system.actorOf(Props(new TestMonitor("instance_2", 15009)))
    val instance3 = SmsSystem.system.actorOf(Props(new TestMonitor("instance_3", 15001)))
    SmsSystem.registerMonitor(instance1)
    SmsSystem.registerMonitor(instance2)
    SmsSystem.registerMonitor(instance3)
    Thread.sleep(300000) // watching monitor
  }

  def testAddRemoteActor() {
    SmsSystem.startAsMaster(8999, false)
    val address = AddressFromURIString("akka.tcp://masterSys@192.168.1.105:10001")
    val actorRef = SmsSystem.system.actorOf(Props(new TestMonitor("instance_1", 15007)).withDeploy(Deploy(scope = RemoteScope(address))), "remoteActor")
    println(actorRef.path)
    Thread.sleep(3000)
  }

  def testAddActorToSlave() {
    SmsSystem.startAsMaster(8999, false, testMasterConf)
    Thread.sleep(1500) //wait for starting
    new Thread {
      override def run {
        SmsSystem.startAsSlave("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", 10010, false, testSlaveConf)
      }
    }.start
    Thread.sleep(1500) //wait for starting
    val actorPath = SmsSystem.addActor(AddMsg("testActor", "akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave", "com.baidu.bpit.akka.MyClusterActor", null))
    Thread.sleep(1500) //wait to completed
    SmsSystem.forwardMsg(actorPath, "test")
    Thread.sleep(1500) //wait to completed
  }

  def testAddConfigBeforeStartSlave() {
    SmsSystem.startAsMaster(8999, false, testMasterConf)
    Thread.sleep(1500) //wait for starting
    val actorPath = SmsSystem.addActor(AddMsg("testActor", "akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave", "com.baidu.bpit.akka.MyActor", null))
    Thread.sleep(1500) //wait for receiving
    new Thread {
      override def run {
        SmsSystem.startAsSlave("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", 10010, false, testSlaveConf)
      }
    }.start
    Thread.sleep(1500) //wait for starting
    SmsSystem.forwardMsg(actorPath, "test")
    println(SmsSystem.listAllRooting)
    Thread.sleep(1500) //wait to completed
  }

  def testSendMsg() {
    new Thread {
      override def run {
        SmsSystem.startAsMaster(8999, false, testMasterConf)
      }
    }.start
    Thread.sleep(1500) //wait for starting
    SmsSystem.forwardMsg("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", new Parameters {})
    Thread.sleep(1500) //wait to completed
  }

  def testUpdateConf() {
    val actorName = "testActor"
    val slavePath = "akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave"
    val actorPath = s"$slavePath/$actorName"
    SmsSystem.startAsMaster(8999, false, testMasterConf)
    SmsSystem.addActor(actorName, slavePath, "com.baidu.bpit.akka.MyClusterActor", null)
    assert(SmsSystem.getParam("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave/testActor") == null)
    SmsSystem.updateConfig(actorPath, "conf","newConfig",true)
    Thread.sleep(1000)
    assert(SmsSystem.getParam("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave/testActor") == "newConfig")
    SmsSystem.updateConfig(actorPath,"conf", InitMsg("newConfig"),true)
    Thread.sleep(1000)
    assert(SmsSystem.getParam("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave/testActor") == InitMsg("newConfig"))
  }


  def testSendClosure(){
    val f: String => Array[String] = _.split(",")
    new Thread {
      override def run {
        SmsSystem.startAsMaster(8999, false, testMasterConf)
        Thread.sleep(1500)
        val actorPath = SmsSystem.addActor(AddMsg("testActor", "akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave", "com.baidu.bpit.akka.MyActor", null))
        Thread.sleep(4500) //wait to completed
        println(SmsSystem.askMsg(actorPath, f))
        Thread.sleep(4500)
      }
    }.start
//    SmsSystem.startAsMaster(8999, false, testMasterConf)
     //wait for starting
    Thread.sleep(2500)

    SmsSystem.startAsSlave("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", 10010, false, testSlaveConf)

    Thread.sleep(15000) //wait to completed
  }


  @After
  def afterTest() {
    SmsSystem.shutDown(1000, 5000)
  }

}