package org.nicta.wdy.hdm.executor

import java.util.concurrent.{BlockingQueue, TimeUnit, LinkedBlockingDeque}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.nicta.wdy.hdm._
import org.nicta.wdy.hdm.functions.{Aggregatable, Aggregator, DualInputFunction}
import org.nicta.wdy.hdm.io.{Path, BufferedBlockIterator}
import org.nicta.wdy.hdm.message.FetchSuccessResponse
import org.nicta.wdy.hdm.model.{HDM, DDM, DataDependency}
import org.nicta.wdy.hdm.server.DependencyManager
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Block}
import org.nicta.wdy.hdm.utils.Utils

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
                                                               keepPartition: Boolean = true,
                                                               partitioner: Partitioner[R] = null) extends ParallelTask[R] {


  final val inTypeOne = classTag[T]

  final val inTypeTwo = classTag[U]

  final val outType = classTag[R]

  override lazy val input = input1 ++ input2

  override def call(): Seq[DDM[_, R]] = {
    runTaskAsynchronously()

  }

  def runTaskIteratively(): Seq[DDM[_, R]] ={
    val input1Iter = new BufferedBlockIterator[T](input1)
    val input2Iter = new BufferedBlockIterator[U](input2)
    val res = func.aggregate((input1Iter, input2Iter), mutable.Buffer.empty[R])
    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms
  }

  def runTaskAsynchronously(): Seq[DDM[_, R]] = {
    // todo be implemented using DataLoader and loading queue
    log.debug(s"Preparing input data for task: [${(taskId, func)}] ")
    val classLoader = DependencyManager().getClassLoader(appId, version)
    var res: Buf[R] = Buf.empty[R]
//    val blockFetchingCounter = new AtomicInteger(0)
    val aggregationCounter = new AtomicInteger(0)
    val fetchingWatcher = new AtomicBoolean(false)
    val inputQueue = new LinkedBlockingDeque[AnyRef]
    val inputIdxMap = new mutable.HashMap[String, Int]()

    // save the indexes mapping from block Id to their input position
    input1.foreach(in => inputIdxMap.put(in.id, 1))
    input2.foreach(in => inputIdxMap.put(in.id, 2))


    // aggregator semantic
    def readAndAggregate[T: ClassTag, U: ClassTag](aggregator: Aggregator[(Arr[T], Arr[U]), Buf[R]], queue:BlockingQueue[AnyRef]): Unit = {
      val received = queue.poll(60, TimeUnit.SECONDS)
      val emptyInput1 = Seq.empty[T].toIterator
      val emptyInput2 = Seq.empty[U].toIterator
      log.info(s"start processing FetchResponse: ${received}.")
      val block = received match {
        case resp: FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[_]](resp.data, classLoader)
        case blk: Block[_] => blk.asInstanceOf[Block[_]]
      }
      inputIdxMap.get(block.id) match {
        case Some(1) =>
          val data = (block.asInstanceOf[Block[T]].data.toIterator, emptyInput2)
          aggregator.aggregate(data)
        case Some(2) =>
          val data = (emptyInput1, block.asInstanceOf[Block[U]].data.toIterator)
          aggregator.aggregate(data)
      }
      val count = aggregationCounter.incrementAndGet()
      log.info(s"finished aggregation: $count/${input.length}.")
    }

    HDMBlockManager.loadBlocksIntoQueue(input, inputQueue, fetchingWatcher)

    func match {
      case aggregator: Aggregator[(Arr[T], Arr[U]), Buf[R]] =>
        log.info(s"Running aggregation with Aggregator..")
        aggregator.init(Buf.empty[R])
        while (!fetchingWatcher.get()) {
          readAndAggregate(aggregator, inputQueue)
        }
        while (!inputQueue.isEmpty) {
          readAndAggregate(aggregator, inputQueue)
        }
        res = aggregator.result

      case aggregation: Aggregatable[(Arr[T], Arr[U]), Buf[R]] =>
        log.info(s"Running aggregation as Aggregatable functions..")
    }

    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms
  }


}
