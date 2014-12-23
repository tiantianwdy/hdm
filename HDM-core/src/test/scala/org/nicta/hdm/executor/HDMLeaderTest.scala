package org.nicta.hdm.executor

import org.junit.{After, Test}
import org.nicta.wdy.hdm.executor.HDMContext
import com.baidu.bpit.akka.server.SmsSystem
import com.baidu.bpit.akka.messages.{AddMsg, Query}
import org.nicta.wdy.hdm.model.HDM
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import akka.actor.{Props, Actor}
import akka.pattern.{ask, pipe}
import akka.actor.Actor.Receive

/**
 * Created by Tiantian on 2014/12/19.
 */
class HDMLeaderTest extends ClusterTestSuite{

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+")

  val text2 =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")


  def testForDebugging{
    HDMContext.startAsMaster(8999, testMasterConf)
    val rootPath = SmsSystem.rootPath
    println(rootPath)
    //    val addmsg1 = AddMsg(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerLeader", null)
    //    val res1 = SmsSystem.askMsg("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", addmsg1).getOrElse("no response")
    //    println(res1)

    //    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster","org.nicta.wdy.hdm.coordinator.BlockManagerLeader", null)
    //    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorLeader", null)

    val res = SmsSystem.askMsg("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster", Query("smsSystem/allActors", "", "")).getOrElse("no response")

    println(res)
  }


  /**
   *
   */
  @Test
  def testLeaderStart(){
    HDMContext.startAsMaster(8999, testMasterConf)
    val rootPath = SmsSystem.rootPath
    println(rootPath)

    Thread.sleep(1500)
  }

  @Test
  def testLocalExecution(){
    HDMContext.init()
    val resActor = SmsSystem.system.actorOf(Props[ResultsReceiver])
    Thread.sleep(1500)
    val hdm = HDM.horizontal(text, text2)
    val wordCount = hdm.map(w => (w,1)).reduceByKey(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

   wordCount.compute() onComplete  {
      case Success(hdm) =>
        println("Job completed and received response:" + hdm)
        hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

    Thread.sleep(500000)
  }


  @After
  def after(){
    HDMContext.shutdown()
  }
}


class ResultsReceiver extends Actor {

  override def receive: Receive = {
    case hdm:HDM[_,_] => hdm.sample().foreach(println(_))
    case x => println("unhandled msg:" + x ); unhandled(x)
  }
}
