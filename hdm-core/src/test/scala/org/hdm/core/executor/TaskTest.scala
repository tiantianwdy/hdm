package org.hdm.core.executor

import org.hdm.akka.server.SmsSystem
import org.hdm.core.functions.ParallelFunction
import org.hdm.core.io.Path
import org.hdm.core.model._
import org.hdm.core.storage.HDMBlockManager
import org.junit.Test

import scala.util.{Failure, Success}

/**
 * Created by tiantian on 27/12/14.
 */
class TaskTest extends ClusterTestSuite {

  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  @Test
  def testHDFSLoadTask(): Unit = {
    implicit val executorContext = ClusterExecutorContext()
    hDMContext.init()
    val maxTaskNum = 2
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/")
    val rootPath = SmsSystem.rootPath
    println(rootPath)
    val hdm = HDM(path)
    val hdms = hDMContext.explain(hdm, 4).physicalPlan
    hdms foreach (println(_))
    var curNum = 0
    hdms foreach { h =>
      HDMBlockManager().addRef(h)
      if (h.isInstanceOf[DFM[_,_]] && curNum < maxTaskNum){
        val dfm = h.asInstanceOf[DFM[_,_]]
        val task = Task(appId = appContext.appName, version = appContext.version,
          taskId = h.id, exeId = "",
          input = h.children.map(h => HDMInfo(h)),
          dep = h.dependency,
          func = h.func.asInstanceOf[ParallelFunction[dfm.inType.type, dfm.outType.type]],
          appContext = appContext,
          blockContext = hDMContext.blockContext())
        ClusterExecutor.runTaskConcurrently(task)
        curNum += 1
      }
    }
   Thread.sleep(500000)
  }

  @Test
  def testHDFSJob(): Unit ={
    implicit val executorContext = ClusterExecutorContext()

    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00001")
    hDMContext.init()
    val hdm = HDM(path)
    hdm.compute(4) onComplete {
      case Success(hdm) =>
        println("Job completed and received response:" + hdm)
//        hdm.asInstanceOf[HDM[_,_]].sample(10).foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }
    Thread.sleep(500000)
  }

}
