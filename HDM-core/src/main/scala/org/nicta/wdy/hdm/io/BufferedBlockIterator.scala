package org.nicta.wdy.hdm.io

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyConnectionManager
import org.nicta.wdy.hdm.message.FetchSuccessResponse
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager, BlockRef}
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * This iterator is used to read a set of distributed DDMs but acts as a local iterator
 * implemented as a cycle buffer with size factor
 * @param bufferSize
 * @param blockRefs
 * @tparam A
 */
class BufferedBlockIterator[A:ClassTag](val blockRefs: Seq[Path], val bufferSize:Int = 100000) extends BufferedIterator[A] with Logging{

  val blockCounter = new AtomicInteger(0)
  val readingOffset = new AtomicInteger(0)
  val fetchingCompleted = new AtomicBoolean(false)
  val isReading = new AtomicBoolean(false)
  val inputQueue = new LinkedBlockingDeque[A]


  def this(hdms:Seq[HDM[_,_]]){
    this(hdms.flatMap(_.blocks).map(Path(_)), 100000)
  }

  def reset() = {
    blockCounter.set(0)
    readingOffset.set(0)
    inputQueue.clear()
    fetchingCompleted.set(false)
  }

  override def head: A = next()

  override def next(): A = {
    if(hasNext){
      if(inputQueue.size() < bufferSize && readingOffset.get() < blockRefs.length && !isReading.get()){
        isReading.set(true)
        loadNextBlock(blockRefs(readingOffset.getAndIncrement()))
        inputQueue.take()
      } else {
        inputQueue.take()
      }
    } else throw new RuntimeException("Null iterator")
  }

  override def hasNext: Boolean = {
    !inputQueue.isEmpty || !fetchingCompleted.get()
  }


  def loadNextBlock(blockPath: Path) = {
    log.info(s"Fetching block from ${blockPath} ...")
    HDMBlockManager.loadBlockAsync(blockPath, Seq(blockPath.name), blockHandler, fetchHandler)
  }

  val blockHandler = (blk:Block[_]) => {
    if (blockCounter.incrementAndGet() >= blockRefs.length) {
      fetchingCompleted.set(true)
    }
    inputQueue.addAll(blk.asInstanceOf[Block[A]].data)
    isReading.set(false)
    log.info(s"Fetched block:${blk.id}, progress: (${blockCounter.get}/${blockRefs.length}).")
  }

  val fetchHandler = (resp:FetchSuccessResponse) => {
    if (blockCounter.incrementAndGet() >= blockRefs.length) {
      fetchingCompleted.set(true)
    }
    inputQueue.addAll(serializeBlock(resp))
    isReading.set(false)
    log.info(s"Received fetch response:${resp.id} with size ${resp.length}, progress: (${blockCounter.get}/${blockRefs.length}).")
  }

  def serializeBlock(received: Any):Seq[A] ={
    val block = received match {
      case resp:FetchSuccessResponse => HDMContext.defaultSerializer.deserialize[Block[A]](resp.data)
      case blk: Block[_] => blk.asInstanceOf[Block[A]]
      case x:Any => Block(Seq.empty[A])
    }
    block.data
  }
}
