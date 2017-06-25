package org.hdm.core.executor

import java.util.concurrent.{ConcurrentHashMap, BlockingQueue, TimeUnit, LinkedBlockingDeque}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.hdm.core._
import org.hdm.core.context.{HDMContext, BlockContext, AppContext}
import org.hdm.core.functions.{Aggregatable, Aggregator, DualInputFunction}
import org.hdm.core.io.{Path, BufferedBlockIterator}
import org.hdm.core.message.FetchSuccessResponse
import org.hdm.core.model.{HDMInfo, HDM, DDM, DataDependency}
import org.hdm.core.server.DependencyManager
import org.hdm.core.storage.{HDMBlockManager, Block}
import org.hdm.core.utils.Utils

import scala.collection.mutable
import scala.reflect._

/**
 * Created by tiantian on 23/11/15.
 */
case class TwoInputTask[T: ClassTag, U: ClassTag, R: ClassTag](appId: String, version:String,
                                                               exeId:String,
                                                               taskId: String,
                                                               input1: Seq[HDM[T]],
                                                               input2: Seq[HDM[U]],
                                                               func: DualInputFunction[T, U, R],
                                                               dep: DataDependency,
                                                               idx:Int = 0,
                                                               keepPartition: Boolean = true,
                                                               partitioner: Partitioner[R] = null,
                                                               appContext: AppContext,
                                                               var blockContext: BlockContext) extends ParallelTask[R] {


  final val inTypeOne = classTag[T]

  final val inTypeTwo = classTag[U]

  final val outType = classTag[R]

  override def input = (input1 ++ input2).map(h => HDMInfo(h))

  override def call(): Seq[DDM[_, R]] = try {
    func.setTaskContext(TaskContext(this))
    runTaskAsynchronously()
  } finally {
    func.removeTaskContext()
  }

  def runTaskIteratively(): Seq[DDM[_, R]] ={
    val classLoader = DependencyManager().getClassLoader(appId, version)
    val input1Iter = new BufferedBlockIterator[T](input1, classLoader)
    val input2Iter = new BufferedBlockIterator[U](input2, classLoader)
    val res = func.aggregate((input1Iter, input2Iter), mutable.Buffer.empty[R])
    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res, appContext, blockContext, hdmContext))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2, appContext, blockContext, hdmContext)).toSeq
    }
    ddms
  }

  def runTaskAsynchronously(): Seq[DDM[_, R]] = {
    // todo be implemented using DataLoader and loading queue
    log.info(s"Preparing input data for task: [${(taskId, func)}] ")
    val classLoader = DependencyManager().getClassLoader(appId, version)
    var res: Buf[R] = Buf.empty[R]
//    val blockFetchingCounter = new AtomicInteger(0)
    val aggregationCounter = new AtomicInteger(0)
    val fetchingWatcher = new AtomicBoolean(false)
    val inputQueue = new LinkedBlockingDeque[AnyRef]
    val inputIdxMap = new ConcurrentHashMap[String, Int]()

    // save the indexes mapping from block Id to their input position
    input1.foreach(in => inputIdxMap.put(in.id, 1))
    input2.foreach(in => inputIdxMap.put(in.id, 2))

    // aggregator semantic
    def readAndAggregate[T: ClassTag, U: ClassTag](aggregator: Aggregator[(Arr[T], Arr[U]), Buf[R]], queue:BlockingQueue[AnyRef]): Unit = {
      val received = queue.take()
      val emptyInput1 = Seq.empty[T].toIterator
      val emptyInput2 = Seq.empty[U].toIterator
      log.info(s"[${taskId}}] start processing FetchResponse: ${received}.")
      val block = received match {
        case resp: FetchSuccessResponse => HDMContext.DEFAULT_SERIALIZER.deserialize[Block[_]](resp.data, classLoader)
        case blk: Block[_] => blk.asInstanceOf[Block[_]]
      }
      inputIdxMap.get(block.id) match {
        case 1 =>
          val data = (block.asInstanceOf[Block[T]].data.toIterator, emptyInput2)
          log.info(s"[${taskId}}] aggregating from input 1 ...")
          aggregator.aggregate(data)
        case 2 =>
          val data = (emptyInput1, block.asInstanceOf[Block[U]].data.toIterator)
          log.info(s"[${taskId}}] aggregating from input 2 ...")
          aggregator.aggregate(data)
      }
      val count = aggregationCounter.incrementAndGet()
      log.info(s"[${taskId}}] finished aggregation: $count/${input.length}.")
    }

    HDMBlockManager.loadBlocksIntoQueue(input, inputQueue, fetchingWatcher)

    func match {
      case aggregator: Aggregator[(Arr[T], Arr[U]), Buf[R]] =>
        log.info(s"[${taskId}}] Running aggregation with Aggregator..")
        aggregator.init(Buf.empty[R])
        while (!fetchingWatcher.get()) {
          readAndAggregate(aggregator, inputQueue)
        }
        log.info(s"[${taskId}}] Fetching process completed..")
        while (!inputQueue.isEmpty) {
          readAndAggregate(aggregator, inputQueue)
        }
        log.info(s"[${taskId}}] Obtaining results from Aggregator:[${aggregator.getClass}}].")
        res = aggregator.result

      case aggregation: Aggregatable[(Arr[T], Arr[U]), Buf[R]] =>
        log.info(s"[${taskId}}] Running aggregation as Aggregatable functions..")
    }

    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res, appContext, blockContext, hdmContext))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2, appContext, blockContext, hdmContext)).toSeq
    }
    ddms
  }


}
