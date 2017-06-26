package org.hdm.core.io

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{LinkedBlockingDeque, Semaphore}
import org.hdm.core.context.HDMContext
import org.hdm.core.message.FetchSuccessResponse
import org.hdm.core.model.HDM
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.hdm.core.utils.Logging

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * This iterator is used to read a set of distributed DDMs but acts as a local iterator
 * implemented as a cycle buffer with size factor
 *
 * @param bufferSize
 * @param blockRefs
 * @tparam A
 */
class BufferedBlockIterator[A:ClassTag](val blockRefs: Seq[Path], 
                                        val bufferSize:Int = 100000,
                                        val classLoader: ClassLoader) extends BufferedIterator[A] with Logging{

  val blockCounter = new AtomicInteger(0)
  val readingOffset = new AtomicInteger(0)
  val fetchingCompleted = new AtomicBoolean(false)
  val isReading = new AtomicBoolean(false)
  val inputQueue = new LinkedBlockingDeque[A]
  val waitForReading = new Semaphore(1)

  def this(hdms:Seq[HDM[_]]){
    this(hdms.flatMap(_.blocks).map(Path(_)), 100000, ClassLoader.getSystemClassLoader)
  }

  def this(hdms:Seq[HDM[_]], classLoader: ClassLoader){
    this(hdms.flatMap(_.blocks).map(Path(_)), 100000, classLoader)
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
      if(inputQueue.size() < bufferSize && readingOffset.get() < blockRefs.length){
        loadNextBlock(blockRefs(readingOffset.getAndIncrement()))
        if(inputQueue.nonEmpty){
          inputQueue.take()
        } else {
          next()
        }
      } else {
        inputQueue.take()
      }
    } else null.asInstanceOf[A]
  }

  override def hasNext: Boolean = {
    if (inputQueue.nonEmpty) true
    else {
      if (readingOffset.get() < blockRefs.length) {
        loadNextBlock(blockRefs(readingOffset.getAndIncrement()))
        if (inputQueue.nonEmpty) true
        else {
          hasNext
        }
      } else if (readingOffset.get() == blockRefs.length && isReading.get()) {
        log.warn(s"Waiting for loading the last block...")
        waitForReading.acquire()
        log.warn(s"Completed waiting for loading the last block...")
        if (inputQueue.nonEmpty) true else false
      } else false
    }
  }


  def loadNextBlock(blockPath: Path) = {
    log.info(s"waiting for loading block...")
    waitForReading.acquire()
    isReading.set(true)
    log.info(s"waiting completed, start loading next block..")
    log.info(s"Fetching block from ${blockPath} ...")

    HDMBlockManager.loadBlockAsync(blockPath, Seq(blockPath.name), blockHandler, fetchHandler)
  }

  val blockHandler = (blk:Block[_]) => {
    if (blockCounter.incrementAndGet() >= blockRefs.length) {
      fetchingCompleted.set(true)
    }
    inputQueue.addAll(blk.asInstanceOf[Block[A]].data)
    isReading.set(false)
    log.info(s"Fetched block:${blk.id} with length ${blk.size}, progress: (${blockCounter.get}/${blockRefs.length}).")
    waitForReading.release()
  }

  val fetchHandler = (resp:FetchSuccessResponse) => {
    if (blockCounter.incrementAndGet() >= blockRefs.length) {
      fetchingCompleted.set(true)
    }
    val data = deserializeBlock(resp)
    inputQueue.addAll(data)
    isReading.set(false)
    log.info(s"Received fetch response:${resp.id} with ${data.length} elements, progress: (${blockCounter.get}/${blockRefs.length}).")
//    if(waitForReading.hasQueuedThreads())
    waitForReading.release()
   }

  def deserializeBlock(received: Any):Seq[A] = {
    val block = received match {
      case resp:FetchSuccessResponse =>
        log.trace(s"Class loader: ${classLoader}")
        HDMContext.DEFAULT_SERIALIZER.deserialize[Block[A]](resp.data, classLoader)
      case blk: Block[_] =>
        blk.asInstanceOf[Block[A]]
      case x:Any => Block(Seq.empty[A])
    }
    block.data
  }
}
