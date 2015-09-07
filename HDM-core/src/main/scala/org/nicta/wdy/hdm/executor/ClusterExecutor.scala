package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.io.{DataParser, HDMIOManager, Path}
import org.nicta.wdy.hdm.model.{OneToN, OneToOne, DDM}
import org.nicta.wdy.hdm.storage.HDMBlockManager
import org.nicta.wdy.hdm.utils.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
 * Created by tiantian on 7/09/15.
 */
object ClusterExecutor extends Logging{


  val blockManager = HDMBlockManager()

  val ioManager = HDMIOManager()

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



}
