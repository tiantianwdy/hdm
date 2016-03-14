package org.nicta.hdm.server

import java.io.File

import com.baidu.bpit.akka.server.SmsSystem
import org.junit.Test
import org.nicta.wdy.hdm.executor.{HDMContext, DynamicDependencyThreadFactory}
import org.nicta.wdy.hdm.message.SerializedJobMsg
import org.nicta.wdy.hdm.serializer.SerializableByteBuffer
import org.nicta.wdy.hdm.server.DependencyManager

/**
 * Created by tiantian on 8/03/16.
 */
class DependencyManagerTest {

  @Test
  def testLoadGlobalDependency(): Unit = {
    val start = System.currentTimeMillis()
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    val url = new File(file).toURI.toURL
    DependencyManager.loadGlobalDependency(Array(url))

    println(Class.forName("org.nicta.wdy.hdm.examples.IterationBenchmark"))
    val end = System.currentTimeMillis() - start
    println(s"loading completed in $end ms.")
  }

  @Test
  def testLoadThreadDependency(): Unit = {
    val start = System.currentTimeMillis()
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    val url = new File(file).toURI.toURL
    DynamicDependencyThreadFactory.addGlobalDependency(Array(url))
    HDMContext.executionContext.execute( new Runnable {
      override def run(): Unit = {
        println(Class.forName("org.nicta.wdy.hdm.examples.IterationBenchmark", false, Thread.currentThread().getContextClassLoader))
      }
    })
    val end = System.currentTimeMillis() - start
    println(s"loading completed in $end ms.")
    Thread.sleep(1000)
  }

  @Test
  def testSubmitDependencyByMsg(): Unit ={

    val start = System.currentTimeMillis()
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    SmsSystem.startAsClient(master, 20001)
    Thread.sleep(100)
    DependencyManager.submitAppByPath(master, "hdm-examples", "0.0.1", file, "dwu")
    Thread.sleep(5000)
  }

  @Test
  def testSendSerializedJob(): Unit ={

    val start = System.currentTimeMillis()
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    SmsSystem.startAsClient(master, 20001)
    val serJob = HDMContext.defaultSerializer.serialize(master).array()
    val jobMsg = SerializedJobMsg("hdm-examples", "0.0.1", serJob, "akka.tcp://slaveSys@127.0.0.1:20001/user/smsSlave"+ "/"+ HDMContext.JOB_RESULT_DISPATCHER, 2)
    Thread.sleep(100)
    SmsSystem.askMsg(master, jobMsg)
    Thread.sleep(5000)
  }
}
