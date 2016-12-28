package org.hdm.core.executor

import java.util.concurrent.Callable

import org.hdm.core.io.{DataParser, HDMIOManager, Path}
import org.hdm.core.model.{OneToN, OneToOne, DDM}
import org.hdm.core.storage.HDMBlockManager
import org.hdm.core.utils.Logging

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
 * Created by tiantian on 7/09/15.
 */
object ClusterExecutor extends Logging{

  val CORES = Runtime.getRuntime.availableProcessors

  val blockManager = HDMBlockManager()

  val ioManager = HDMIOManager()

  def  runTask[R: ClassTag](task: Callable[Seq[DDM[_,R]]])(implicit executionContext: ExecutionContext): Future[Seq[DDM[_,_]]] = {
      Future {
        task.call()
      }
  }

  def runTaskSynconized[I: ClassTag, R: ClassTag](task: Task[I, R])(implicit executionContext: ExecutionContext): Future[Seq[DDM[_,_]]] = {
    if (task.dep == OneToOne || task.dep == OneToN)
      Future {
        task.call()
//        task.runTaskIteratively().map(bl => bl.toURL)
      }
    else
      Future {
        task.runShuffleTaskAsync()
//        task.runTaskIteratively()
      }
//      runTaskConcurrently(task)
  }

  def runTaskConcurrently[I: ClassTag, R: ClassTag](task: Task[I, R])(implicit executionContext: ExecutionContext): Future[Seq[DDM[_,_]]] = {
    // prepare input data from cluster
    log.info(s"Preparing input data for task: [${(task.taskId, task.func)}] ")
    val input = task.input
      //.map(in => blockManager.getRef(in.id)) // update the states of input blocks
//    val updatedTask = task.copy(input = input.asInstanceOf[Seq[HDM[_, I]]])
//    val remoteBlocks = input.filterNot(ddm => blockManager.isCached(ddm.id))
    val futureBlocks = input.map { ddm =>
      if (!blockManager.isCached(ddm.id)) {
        ddm.location.protocol match {
          case Path.AKKA =>
            //todo replace with using data parsers readAsync
            log.info(s"Asking block ${ddm.location.name} from ${ddm.location.parent}")
            ioManager.askBlock(ddm.location.name, ddm.location.parent) // this is only for hdm
          case Path.HDFS => Future {
            val bl = DataParser.readBlock(ddm.location)
            //          blockManager.add(ddm.id, bl)
            //          ddm.id
            bl
          }
        }
      } else Future {
        blockManager.getBlock(ddm.id)
      }
    }

      Future.sequence(futureBlocks) map { in =>
        log.info(s"Input data preparing finished, the task starts running: [${(task.taskId, task.func)}] ")
        val results = task.runWithInput(in)
        log.info(s"Task completed, with output id: [${results.map(_.toURL)}] ")
        results
      }

  }

  def runMockTask[R: ClassTag](task: ParallelTask[R])(implicit executionContext: ExecutionContext): Future[Seq[DDM[_,_]]] = {
    Future {
      val interval = 1000 + Math.random() * 5000
      Thread.sleep(interval.toLong)
      val ddms = if (task.partitioner == null || task.partitioner.isInstanceOf[KeepPartitioner[_]]) {
        Seq(DDM[R](task.taskId, mutable.Buffer.empty[R], task.appContext, task.blockContext, null))
      } else {
        val partitionNum = task.partitioner.partitionNum
        val outputData = Seq.fill(partitionNum){
          mutable.Buffer.empty[R]
        }
        var i = -1
        outputData.map{
          i += 1
          seq => DDM(task.taskId + "_p" + i, seq, task.appContext, task.blockContext, null)
        }.toSeq
      }
      ddms
    }
  }



}
