package org.nicta.hdm.executor

import com.baidu.bpit.akka.server.SmsSystem
import org.junit.Test
import org.nicta.wdy.hdm.coordinator.ClusterExecutor
import org.nicta.wdy.hdm.executor.{ClusterExecutorContext, Task, HDMContext}
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{DFM, DDM, HDM}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.util.{Failure, Success}

/**
 * Created by tiantian on 27/12/14.
 */
class TaskTest extends ClusterTestSuite {


  @Test
  def testHDFSLoadTask(): Unit = {
    implicit val executorContext = ClusterExecutorContext()
    HDMContext.init()
    val maxTaskNum = 2
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/")
    val rootPath = SmsSystem.rootPath
    println(rootPath)
    val hdm = HDM(path)
    val hdms = HDMContext.explain(hdm, 4)
    hdms foreach (println(_))
    var curNum = 0
    hdms foreach { h =>
      HDMBlockManager().addRef(h)
      if (h.isInstanceOf[DFM[_,_]] && curNum < maxTaskNum){
        val task = Task(appId = h.id,
          taskId = h.id,
          input = h.children.asInstanceOf[Seq[HDM[_, h.inType.type]]],
          dep = h.dependency,
          func = h.func.asInstanceOf[ParallelFunction[h.inType.type, h.outType.type]])
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
    HDMContext.init()
    val hdm = HDM(path)
    hdm.compute(4) onComplete {
      case Success(hdm) =>
        println("Job completed and received response:" + hdm)
        hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }
    Thread.sleep(500000)
  }

}
