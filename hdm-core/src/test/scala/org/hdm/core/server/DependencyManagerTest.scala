package org.hdm.core.server

import java.io.File

import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.{AppContext, DynamicDependencyThreadFactory, HDMContext}
import org.hdm.core.message.SerializedJobMsg
import org.junit.Test

/**
 * Created by tiantian on 8/03/16.
 */
class DependencyManagerTest {

  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  @Test
  def testLoadGlobalDependency(): Unit = {
    val start = System.currentTimeMillis()
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    val url = new File(file).toURI.toURL
    DependencyManager.loadGlobalDependency(Array(url))

    println(Class.forName("org.hdm.core.examples.IterationBenchmark"))
    val end = System.currentTimeMillis() - start
    println(s"loading completed in $end ms.")
  }

  @Test
  def testLoadThreadDependency(): Unit = {
    val start = System.currentTimeMillis()
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    val url = new File(file).toURI.toURL
    DynamicDependencyThreadFactory.addGlobalDependency(Array(url))
    hDMContext.executionContext.execute( new Runnable {
      override def run(): Unit = {
        println(Class.forName("org.hdm.core.examples.IterationBenchmark", false, Thread.currentThread().getContextClassLoader))
      }
    })
    val end = System.currentTimeMillis() - start
    println(s"loading completed in $end ms.")
    Thread.sleep(1000)
  }

  @Test
  def testSubmitAppByMsg(): Unit ={

    val start = System.currentTimeMillis()
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val file = "/home/tiantian/Dev/workspace/hdm/hdm-benchmark/target/HDM-benchmark-0.0.1.jar"
    SmsSystem.startAsClient(master, 20001)
    Thread.sleep(100)
    DependencyManager.submitAppByPath(master, "hdm-examples", "0.0.1", file, "dwu")
    Thread.sleep(3000)
  }

  @Test
  def testSubmitDependencyByMsg(): Unit ={

    val start = System.currentTimeMillis()
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val file = "/home/tiantian/Dev/lib/hdm/hdm-core/HDM-core-0.0.1.jar"
    SmsSystem.startAsClient(master, 20001)
    Thread.sleep(100)
    DependencyManager.submitDepByPath(master, "hdm-examples", "0.0.3", file, "dwu")
    Thread.sleep(3000)
  }

  @Test
  def testSendSerializedJob(): Unit ={

    val start = System.currentTimeMillis()
    val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    SmsSystem.startAsClient(master, 20001)
    val serJob = hDMContext.defaultSerializer.serialize(master).array()
    val jobMsg = SerializedJobMsg("hdm-examples", "0.0.1", serJob, "akka.tcp://slaveSys@127.0.0.1:20001/user/smsSlave"+ "/"+ HDMContext.JOB_RESULT_DISPATCHER, master, 2)
    Thread.sleep(100)
    SmsSystem.askSync(master, jobMsg)
    Thread.sleep(5000)
  }
}
