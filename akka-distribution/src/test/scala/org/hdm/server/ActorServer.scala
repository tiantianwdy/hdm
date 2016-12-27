package org.hdm.akka.server

import org.hdm.akka.actors.MasterActor
import org.hdm.akka.actors.SlaveActor
import org.hdm.akka.actors.worker.PersistenceActor
import org.hdm.akka.messages.JoinMsg
import org.hdm.akka.messages.StopMsg
import org.hdm.akka.persistence.DefaultPersistenceService
import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala

/**命令行server测试类 已废弃
 * @author wudongyao
 * @date 2013-7-18 
 * @version 0.0.1
 *
 */
@Deprecated
object ActorServer extends App {


  def startUpSlave(masterPath: String, systemName: String, slaveName: String, port: Int): ActorRef = {

    System.setProperty("akka.remote.netty.tcp.port", port.toString)
    val config = ConfigFactory.load()
    val slaveSystem = ActorSystem(systemName, config)
    val masterRooter = slaveSystem.actorFor(masterPath)
    slaveSystem.actorOf(Props(new SlaveActor(masterRooter)), slaveName)
  }

  def startUpMaster(systemName: String, masterName: String, port: Int): ActorRef = {

    System.setProperty("akka.remote.netty.tcp.port", port.toString)
    val config = ConfigFactory.load()
    val masterSystem = ActorSystem(systemName, config)
    val persistenceActor = masterSystem.actorOf(Props(new PersistenceActor(new DefaultPersistenceService)), name = "persistenceActor")
    masterSystem.actorOf(Props(new MasterActor(persistenceActor)), masterName)
  }

  def join(masterPath: String, slavePath: String) {

    System.setProperty("akka.remote.netty.hostname", "")
    System.setProperty("akka.remote.netty.tcp.port", "2552")
    val config = ConfigFactory.load()
    val system = ActorSystem("client", config)
    val slaveActor = system.actorSelection(slavePath)
    slaveActor ! JoinMsg(masterPath = masterPath)
    system.shutdown()
  }

  def addConf(actorPath: String, slavePath: String, typ: String, actorClazz:String,  params: Seq[String]) = ???

  def stop(actorPath: String) {

    System.setProperty("akka.remote.netty.hostname", "")
    System.setProperty("akka.remote.netty.port", "2552")
    val system = ActorSystem()
    val actor = system.actorFor(actorPath)
    actor ! StopMsg
    system.shutdown()
  }

  if (args.length < 2)
    println("not enough parameters")

  args(0) match {
    case "start" => {
      val actorName = args(2)
      val actorPort = args(3)
      args(1) match {
        case "slave" => {
          val masterPath = args(4)
          startUpSlave(masterPath, "slaveSys", actorName, actorPort.toInt)
        }
        case "master" => startUpMaster("masterSys", actorName, actorPort.toInt)
        case _ => println("unknown parameter: " + args(1))
      }
    }
    case "join" => {
      val masterPath = args(1)
      val slavePath = args(2)
      join(masterPath, slavePath)
    }
    case "stop" => {
      val actorPath = args(1)
      stop(actorPath)
    }
    case "add" => {
      addConf(args(1), args(2), args(3), args(4), args.drop(5))
    }
    case _ => println("unknown command:" + args(0))
  }

}

object ActorServerTest extends App {
  val masterName = "smsMaster"
  val paramMaster = Array("start", "master", masterName, "10001")
  val paramSlave = Array("start", "slave", "slave_15010", "15010", "akka.tcp://masterSys@172.22.235.9:10001/user/smsMaster")
  //  val paramJoin =  Array("join", "akka://masterSys@172.22.218.177:15000/user/master", "akka://slaveSys@172.22.218.177:15010/user/slave_15010")
  val paramStop = Array("stop", "akka.tcp://slaveSys@172.22.218.177:15010/user/slave_15010")
  //  val paramAddProxy = Array("add", "akka.tcp://masterSys@172.22.218.177:10001/user/smsMaster", "akka.tcp://slaveSys@172.22.218.177:15010/user/slave_15010", "proxy", "SMPP", "true")
  val paramAddQue = Array("add", "akka.tcp://masterSys@172.22.235.9:10001/user/smsMaster", "akka.tcp://slaveSys@172.22.235.9:15010/user/slave_15010", "queue", "org.hdm.akka.actors.worker.QueActor","15010", "akka://slaveSys@172.22.218.177:15010/user/slave_15010/proxy")
  val paramAddDispatch = Array("add", "akka.tcp://masterSys@172.22.218.177:10001/user/smsMaster", "akka.tcp://slaveSys@172.22.218.177:15010/user/slave_15010", "dispatcher", "15010", "akka://slaveSys@172.22.218.177:15010/user/slave_15010/queue")
  val paramSendSms = Array("sendSms", "akka.tcp://slaveSys@172.22.218.177:15010/user/slave_15010/dispatcher", "15010", "008613522438071", "this is a test message:" + System.currentTimeMillis())

  ActorServer.main(paramMaster)
}