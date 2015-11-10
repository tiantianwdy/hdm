package org.nicta.wdy.hdm.executor

import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}

import org.nicta.wdy.hdm.{Buf, Arr}
import org.nicta.wdy.hdm.io.{HDMIOManager, DataParser, Path}
import org.nicta.wdy.hdm.message.FetchSuccessResponse
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.functions.{ParCombinedFunc, ParallelFunction, DDMFunction_1, SerializableFunction}
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit, Callable}
import org.nicta.wdy.hdm.utils.{Utils, Logging}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.reflect.{ClassTag, classTag}
import scala.util.{Success, Failure}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager, BlockManager, Block}


/**
 * Created by Tiantian on 2014/12/1.
 */
case class Task[I:ClassTag,R: ClassTag](appId:String,
                     taskId:String,
                     input:Seq[HDM[_, I]],
                     func:ParallelFunction[I,R],
                     dep:DataDependency,
                     keepPartition:Boolean = true,
                     partitioner: Partitioner[R] = null,
                     createTime:Long = System.currentTimeMillis())
                     extends Serializable with Callable[Seq[DDM[_,R]]] with Logging{


  final val inType = classTag[I]

  final val outType = classTag[R]

  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)

  override def call() = try {
    if(dep == OneToOne || dep == OneToN)
      runSequenceTask()
    else
      runShuffleTaskAsync()
  } catch {
    case e : Throwable =>
      e.printStackTrace()
      throw e
  }


    def runShuffleTask():Seq[DDM[_,R]]  = {
      val blocks = input.map(ddm => DataParser.readBlock(ddm, true))

      val inputData = blocks.map(_.data.toIterator.asInstanceOf[Arr[I]]).flatten.toIterator
      val ouputData = func.apply(inputData)
      log.debug(s"non-iterative shuffle results: ${ouputData.take(10)}")
      val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
        Seq(DDM[R](taskId, ouputData.toBuffer))
      } else {
        partitioner.split(ouputData).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
      }
      ddms

    }

  def runShuffleTaskAsync():Seq[DDM[_,R]] = {
    log.info(s"Preparing input data for task: [${(taskId, func)}] ")
    var res:Buf[R] = Buf.empty[R]
    val blockCounter = new AtomicInteger(0)
    val countDownWatch = new AtomicInteger(0)
    val fetchingCompleted = new AtomicBoolean(false)
    val inputQueue = new LinkedBlockingDeque[AnyRef]
    val blockHandler = (blk:Block[_]) => {
      if (blockCounter.incrementAndGet() >= input.length) {
        fetchingCompleted.set(true)
      }
      inputQueue.offer(blk)
      log.info(s"Fetched block:${blk.id}, progress: (${blockCounter.get}/${input.length}).")
    }

    val fetchHandler = (resp:FetchSuccessResponse) => {
      if (blockCounter.incrementAndGet() >= input.length) {
        fetchingCompleted.set(true)
      }
      inputQueue.offer(resp)
      log.info(s"Received fetch response:${resp.id} with size ${resp.length}, progress: (${blockCounter.get}/${input.length}).")
    }

    //group block by host address
    val blockByAddress = input.map(_.location).groupBy{p =>
      p.address
    }.map(bl => (Path(bl._1), bl._2.map(_.name)))
    //randomize the request to avoid IO contense
    val remoteBlocks = Utils.randomize(blockByAddress.toSeq)

    for(blocks <- remoteBlocks){
      log.info(s"Fetching block from ${blocks._1} ...")
      HDMBlockManager.loadBlockAsync(blocks._1, blocks._2, blockHandler, fetchHandler)
    }
/*    while (inputIter.hasNext) {
      val input = inputIter.next()
      log.info(s"Fetching block:${input.location} ...")
      HDMBlockManager.loadBlockAsync(input.location, blockHandler)
    }*/

    if (func.isInstanceOf[ParCombinedFunc[I, _, R]]) {
      log.info(s"Running as shuffle aggregation..")
      val tempF = func.asInstanceOf[ParCombinedFunc[I, _, R]]
      val concreteFunc = tempF.asInstanceOf[ParCombinedFunc[I, tempF.mediateType.type, R]]
      var partialRes: Buf[tempF.mediateType.type] = Buf.empty[tempF.mediateType.type]
      while (!fetchingCompleted.get()) {
        val received = inputQueue.poll(60, TimeUnit.SECONDS)
        log.info(s"start processing FetchResponse: ${received}.")
        val block = received match {
          case resp:FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[_]](resp.data)
          case blk: Block[_] => blk.asInstanceOf[Block[_]]
        }
        partialRes = concreteFunc.partialAggregate(block.asInstanceOf[Block[I]].data.toIterator, partialRes)
        val count = countDownWatch.incrementAndGet()
        log.info(s"finished shuffle aggregation: $count/${input.length}.")
      }
      while (!inputQueue.isEmpty) {
        // make sure all the data accepted has been processed
        // there are some racing possibilities for multi-threads if the two conditions are tested together
        val received = inputQueue.poll(60, TimeUnit.SECONDS)
        log.info(s"start processing FetchResponse: ${received}.")
        val block = received match {
          case resp:FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[_]](resp.data)
          case blk: Block[_] => blk.asInstanceOf[Block[_]]
        }
        partialRes = concreteFunc.partialAggregate(block.asInstanceOf[Block[I]].data.toIterator, partialRes)
        val count = countDownWatch.incrementAndGet()
        log.info(s"finished shuffle aggregation: $count/${input.length}.")
      }
      log.trace(s"partial results: ${partialRes.take(10)}")
      res = concreteFunc.postF(partialRes.toIterator).toBuffer
      log.trace(s"shuffle results: ${res.take(10)}")

    } else {
      log.info(s"Running as parallel aggregation..")
      while (!fetchingCompleted.get()) {
        val received = inputQueue.poll(60, TimeUnit.SECONDS)
        log.info(s"start processing FetchResponse: ${received}.")
        val block = received match {
          case resp:FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[_]](resp.data)
          case blk: Block[_] => blk.asInstanceOf[Block[_]]
        }
        res = func.aggregate(block.asInstanceOf[Block[I]].data.toIterator, res)
        val count = countDownWatch.incrementAndGet()
        log.info(s"finished parallel aggregation: $count/${input.length}.")
      }
      while (!inputQueue.isEmpty) {
        // make sure all the data accepted has been processed
        val received = inputQueue.poll(60, TimeUnit.SECONDS)
        log.info(s"start processing FetchResponse: ${received}.")
        val block = received match {
          case resp:FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[_]](resp.data)
          case blk: Block[_] => blk.asInstanceOf[Block[_]]
        }
        res = func.aggregate(block.asInstanceOf[Block[I]].data.toIterator, res)
        val count = countDownWatch.incrementAndGet()
        log.info(s"finished parallel aggregation: $count/${input.length}.")
      }
      log.trace(s"shuffle results: ${res.take(10)}")
    }

    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms
  }


  def runSequenceTask(): Seq[DDM[_, R]] = {
    //load input data
    val start = System.currentTimeMillis()
    val data = input.map { in =>
      val inputData = DataParser.readBlock(in, false)
      //apply function
      log.info(s"Input data preparing finished, task running: [${(taskId, func)}] ")
      log.info(s"Input data size ${inputData.data.size} ")
      val res = func.apply(inputData.asInstanceOf[Block[I]].data.toIterator).toBuffer
      val end = System.currentTimeMillis() - start
      log.info(s"time consumed for function: $end ms.")
      res
    }.flatten
    val end2 = System.currentTimeMillis() - start
    log.info(s"time consumed for flatten: $end2 ms.")
//    log.trace(s"sequence results: ${data.take(10)}")
    //partition as seq of data
    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, data))
    } else {
      partitioner.split(data).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms
  }



  def runTaskIteratively()(implicit executionContext: ExecutionContext): Seq[DDM[_,_]] = {
    log.info(s"Preparing input data for task: [${(taskId, func)}] ")
    val iter = input.iterator
    var res:Buf[R] = Buf.empty[R]
    val inputFinished = new AtomicBoolean(false)
    val inputQueue = new LinkedBlockingDeque[Block[_]]
    Future{
      while(iter.hasNext) {
        val block = DataParser.readBlock(iter.next(), true)
        inputQueue.offer(block)
      }
      inputFinished.set(true)
    }

    if (func.isInstanceOf[ParCombinedFunc[I,_,R]] ) {
      log.info(s"Running as shuffle aggregation..")
      val tempF = func.asInstanceOf[ParCombinedFunc[I,_,R]]
      val concreteFunc = tempF.asInstanceOf[ParCombinedFunc[I,tempF.mediateType.type,R]]
      var partialRes:Buf[tempF.mediateType.type ] = Buf.empty[tempF.mediateType.type ]
      while (!inputFinished.get()) {
        val block = inputQueue.take()
//        concreteFunc.getAggregator().aggregate(block.data.asInstanceOf[Seq[I]].iterator)
        partialRes = concreteFunc.partialAggregate(block.data.asInstanceOf[Arr[I]], partialRes)
      }
      while(!inputQueue.isEmpty){
        val block = inputQueue.take()
        //        concreteFunc.getAggregator().aggregate(block.data.asInstanceOf[Seq[I]].iterator)
        partialRes = concreteFunc.partialAggregate(block.data.asInstanceOf[Arr[I]], partialRes)
      }
//      println(s"partial results: ${partialRes.take(10)}")
//      partialRes =  concreteFunc.getAggregator().getResults
      res = concreteFunc.postF(partialRes.toIterator).toBuffer
      log.debug(s"shuffle results: ${res.take(10)}")

    } else {
      log.info(s"Running as parallel aggregation..")
      while (!inputFinished.get()) {
        val block = inputQueue.take()
        res = func.aggregate(block.data.asInstanceOf[Arr[I]], res)
      }
      while(!inputQueue.isEmpty){
        val block = inputQueue.take()
        res = func.aggregate(block.data.asInstanceOf[Arr[I]], res)
      }
//      println(s"shuffle results: ${res.take(10)}")
    }
    val ddms = if(partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms

  }


  def runWithInput(blks:Seq[Block[_]]) = try {
    //load input data
    //      val inputData = input.flatMap(b => HDMBlockManager.loadOrCompute[I](b.id).map(_.data))
    //      val inputData = input.flatMap(hdm => hdm.blocks.map(Path(_))).map(p => HDMBlockManager().getBlock(p.name).data.asInstanceOf[Seq[I]])
    val inputData = blks.map(_.data.asInstanceOf[Arr[I]])
    //apply function
    val data = func.apply(inputData.toIterator.flatten)
    println(s"sequence results: ${data.take(10)}")
    //partition as seq of data
    val ddms = if(partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, data.toBuffer))
    } else {
      partitioner.split(data).map(seq =>
        DDM(taskId + "_p" + seq._1, seq._2, false)
      ).toSeq
    }
    ddms
    //    DFM(children = input, blocks = blocks.map(_.id), state = Computed, func = func)
  } catch {
    case e : Throwable =>
      e.printStackTrace()
      throw e
  }

}
