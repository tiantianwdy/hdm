package org.nicta.wdy.hdm.examples

import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.executor.{AppContext, HDMContext}
import org.junit.Test
import org.nicta.wdy.hdm.message.{AddHDMsMsg, SerializedJobMsg}
import org.nicta.wdy.hdm.model.ParHDM
import org.nicta.wdy.hdm.serializer.SerializableByteBuffer
import scala.concurrent.{Promise, ExecutionContext}
import ExecutionContext.Implicits.global

/**
 * Created by tiantian on 8/03/16.
 */
class RemoteDependencySubmitTest {

  type Benchmark = org.nicta.wdy.hdm.benchmark.KVBasedPrimitiveBenchmark
  val hDMContext = HDMContext.defaultHDMContext
  val appContext = AppContext.defaultAppContext

  @Test
  def testSendSerializedJob(): Unit ={

    val start = System.currentTimeMillis()
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 2
    hDMContext.NETTY_BLOCK_SERVER_PORT = 9093
    appContext.appName = "hdm-examples"
    appContext.version = "0.0.1"
    hDMContext.startAsClient(context, 20011, 9093)
    Thread.sleep(1500)

    val benchmark = new Benchmark(context)
    val hdm = benchmark.testMap(data)

    val jobBytes = hDMContext.defaultSerializer.serialize(hdm).array
    val encodedJob = jobBytes ++ Array(jobBytes.length.toByte)

    val rootPath =  SmsSystem.rootPath
    hDMContext.declareHdm(Seq(hdm))
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER,
      AddHDMsMsg(appContext.appName , Seq(hdm), rootPath + "/"+ HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[ParHDM[_,_]]]
      case none => null
    }

    val jobMsg = SerializedJobMsg(appContext.appName, appContext.version, jobBytes,
      hDMContext.leaderPath.get() + "/"+ HDMContext.JOB_RESULT_DISPATCHER, hDMContext.leaderPath.get(), parallelism)
    Thread.sleep(100)
    SmsSystem.askAsync(context, jobMsg)
    Thread.sleep(50000)
  }

  @Test
  def testExecuteRemote(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 2
    hDMContext.NETTY_BLOCK_SERVER_PORT = 9093
    appContext.appName = "hdm-examples"
    appContext.version = "0.0.1"
    hDMContext.startAsClient(context, 20011, 9093)
    Thread.sleep(1500)

    val benchmark = new Benchmark(context)
    val hdm = benchmark.testGroupBy(data)
//    HDMContext.submitJob(HDMContext.appName, HDMContext.version, hdm, parallelism) onComplete {
//      case res => println(res)
//    }
    onEvent(hdm, "compute")(parallelism)
    Thread.sleep(50000000)
  }

  def onEvent(hdm:ParHDM[_,_], action:String)(implicit parallelism:Int) = action match {
    case "compute" =>
      val start = System.currentTimeMillis()
      hdm.compute(parallelism).map { hdm =>
        println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
        hdm.blocks.foreach(println(_))
        System.exit(0)
      }

    case "sample" =>
      //      val start = System.currentTimeMillis()
      hdm.sample(25, 500000)foreach(println(_))

    case "collect" =>
      val start = System.currentTimeMillis()
      val itr = hdm.collect()
      println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received results: ${itr.size}")

    case x =>
  }


}
