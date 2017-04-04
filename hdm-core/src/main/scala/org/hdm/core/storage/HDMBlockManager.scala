package org.hdm.core.storage

import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap}

import org.hdm.akka.monitor.SystemMonitorService
import org.hdm.core.executor.HDMContext
import org.hdm.core.io.netty.{NettyBlockServer, NettyConnectionManager}
import org.hdm.core.io.{DataParser, Path}
import org.hdm.core.message.{FetchSuccessResponse, QueryBlockMsg}
import org.hdm.core.model._
import org.hdm.core.utils.{Logging, Utils}

import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/15.
 */
trait HDMBlockManager {

  def getRefs(ids:Seq[String]): Seq[HDM[_]]

  def findRefs(idPattern: String => Boolean): Seq[HDM[_]]

  def declare(br:ParHDM[_,_]): ParHDM[_,_]

  def cache(id:String, bl: Block[_])

  def cacheAll(bm: Map[String, Block[_]])

  def getBlock(id:String):Block[_]

  def getRef (id:String): HDM[_]

  def addRef(br:HDM[_])

  def addAllRef(brs: Seq[ParHDM[_,_]])

  def add(id:String, block:Block[_])

  def addAll(blocks:Map[String, Block[_]])

  def addAll(blocks:Seq[Block[_]])

  def removeRef(id: String)

  def removeBlock(id:String)

  def removeAll(id: Seq[String])

  def isCached(id:String):Boolean = ???

  def checkState(id:String, state:BlockState):Boolean

  def checkAllStates(ids:Seq[String], state:BlockState):Boolean


  def getLocations(ids:Seq[String]):Seq[Path] = {
    ids.map { id =>
      val hdm = getRef(id)
      if (hdm.preferLocation == null)
        hdm.blocks.map(Path(_))
      else Seq(hdm.preferLocation)
    }.flatten
  }

  def getblockSizes(ids:Seq[String]):Seq[Long] = {
    ids.map { id =>
      val hdm = getRef(id)
      hdm match {
        case dfm:DFM[_,_] =>
          val ddms = dfm.blocks.map(Path(_).name)
          getblockSizes(ddms)
        case ddm:DDM[_,_] =>
          Seq(ddm.blockSize)
      }
    }.flatten
  }

}


class DefaultHDMBlockManager(hDMContext: HDMContext) extends HDMBlockManager with Logging{

  import scala.collection.JavaConversions._

  val blockCache = new ConcurrentHashMap[String, Block[_]]()

  val blockRefMap = new ConcurrentHashMap[String, HDM[_]]()

  val releasedBlockSize = new AtomicInteger(0)


  override def checkAllStates(ids: Seq[String], state: BlockState): Boolean = {
    getRefs(ids).forall(_.state == state)
  }

  override def checkState(id: String, state: BlockState): Boolean = {
    val ref = getRef(id)
    if (ref eq null) false
    else ref.state == state
  }

  override def removeAll(ids: Seq[String]): Unit = {
    ids.foreach(removeRef(_))
  }

  override def removeRef(id: String): Unit = {
    removeBlock(id)
    blockRefMap.remove(id)
  }


  override def removeBlock(id: String): Unit = {
    val blk = blockCache.remove(id)
    val dataSize = Block.byteSize(blk)
    HDMBlockManager.cleanup(blk)
    val memReleased = releasedBlockSize.addAndGet(dataSize.toInt)
    if(memReleased > hDMContext.MAX_MEM_GC_SIZE){
      releasedBlockSize.set(0)
      HDMBlockManager.forceGC()
    }
    log.trace(s"JVM freeMem size: ${HDMBlockManager.freeMemMB()} MB.")
  }

  override def addAll(blocks: Map[String, Block[_]]): Unit = {
    blockCache.putAll(blocks)
    log.trace(s"JVM freeMem size: ${HDMBlockManager.freeMemMB()} MB.")
  }

  override def addAll(blocks: Seq[Block[_]]): Unit = {
    blocks.foreach(br => add(br.id, br))
  }

  override def add(id: String, block: Block[_]): Unit = {
    blockCache.put(id, block)
    log.trace(s"JVM freeMem size: ${HDMBlockManager.freeMemMB()} MB.")
  }

  override def addAllRef(brs: Seq[ParHDM[_, _]]): Unit = {
    brs.foreach(addRef(_))
  }

  override def addRef(br: HDM[_]): Unit = {
    blockRefMap.put(br.id, br)
  }

  override def getRef(id: String): HDM[_] = {
    blockRefMap.get(id)
  }

  override def getBlock(id: String): Block[_] = {
    blockCache.get(id)
  }

  override def cacheAll(bm: Map[String, Block[_]]): Unit = addAll(bm)

  override def cache(id: String, bl: Block[_]): Unit = add(id, bl)

  override def declare(br: ParHDM[_, _]): ParHDM[_, _] = {
    addRef(br)
    br
  }

  override def findRefs(idPattern: (String) => Boolean): Seq[HDM[_]] = {
    getRefs(blockRefMap.keySet().filter(idPattern).toSeq)
  }

  override def getRefs(ids: Seq[String]): Seq[HDM[_]] = {
    ids.map(blockRefMap.get(_))
  }

  override def isCached(id: String): Boolean = {
    blockCache.containsKey(id)
  }
}

object HDMBlockManager extends Logging{

  var defaultManager = new DefaultHDMBlockManager(HDMContext.defaultHDMContext) // todo change to loading according to the config

  var defaultBlockServer =  {
    val hDMContext = HDMContext.defaultHDMContext
    val compressor = if (hDMContext.BLOCK_COMPRESS_IN_TRANSPORTATION) hDMContext.compressor
    else null
    new NettyBlockServer(hDMContext.NETTY_BLOCK_SERVER_PORT,
      hDMContext.NETTY_BLOCK_SERVER_THREADS,
      defaultManager,
      hDMContext.defaultSerializer,
      compressor)
  }


  def localBlockServerAddress:String = {
    val localAddr = new InetSocketAddress(NettyConnectionManager.localHost, HDMContext.defaultHDMContext.NETTY_BLOCK_SERVER_PORT)
    localAddr.getHostString  + ":" + localAddr.getPort
  }

  def initBlockServer(hDMContext: HDMContext) = {
    defaultManager = new DefaultHDMBlockManager(hDMContext)
    val compressor = if (hDMContext.BLOCK_COMPRESS_IN_TRANSPORTATION) hDMContext.compressor
    else null
    defaultBlockServer =  new NettyBlockServer(hDMContext.NETTY_BLOCK_SERVER_PORT,
      hDMContext.NETTY_BLOCK_SERVER_THREADS,
      defaultManager,
      hDMContext.defaultSerializer,
      compressor)
    defaultBlockServer.init()
    defaultBlockServer.start()
  }

  def shutdown(): Unit ={
    defaultBlockServer.shutdown()
  }

  def apply():HDMBlockManager = defaultManager

  def loadOrCompute[T: ClassTag](refID: String):Seq[Block[T]] = {
    val id = Path(refID).name
    val ref = defaultManager.getRef(id)
    if (ref != null && ref.state == Computed)
      ref.blocks.map(url => loadBlock[T](url))
    else Seq.empty[Block[T]]
  }

  def loadBlock[T: ClassTag](url:String):Block[T] ={
    val path = Path(url)
//    val bl = defaultManager.getBlock(id)*/
    val bl = DataParser.readBlock(path, ClassLoader.getSystemClassLoader)
    if(bl ne null) bl.asInstanceOf[Block[T]]
    else {
      //compute the block
      //todo change to submit a computing task
      Block(Seq.empty[T])
    }
  }

  def loadBlockAsync(path: Path, blockIds: Seq[String], blockHandler: Block[_] => Unit, remoteHandler: FetchSuccessResponse => Unit): Unit = {
    val (localBlks, remoteBlks) = blockIds.span(id => defaultManager.isCached(id))
    log.trace(s"local blocks:$localBlks")
    log.trace(s"remote blocks:$remoteBlks")
    for (bId <- localBlks) {
      log.info(s"block:${bId} is at local")
      blockHandler.apply(defaultManager.getBlock(bId))
    }
    // change to support of different protocol
    if(remoteBlks.nonEmpty){
      val blockFetcher = NettyConnectionManager.getInstance.getConnection(path.host, path.port)
      val success = blockFetcher.sendRequest(QueryBlockMsg(remoteBlks, path.host + ":" + path.port), remoteHandler)
      if (!success) throw new RuntimeException("send block request failed to path:" + path)
      //      NettyConnectionManager.getInstance.recycleConnection(path.host, path.port, blockFetcher)
    }
  }

  def loadBlockAsync(path:Path, blockHandler: Block[_] => Unit, remoteHandler: FetchSuccessResponse => Unit): Unit ={
    loadBlockAsync(path, Seq(path.name), blockHandler,  remoteHandler)
  }

  /**
   * Incrementally loading blocks and add to a blocking queue, after completed set the watcher to true
   *
   * @param hdms
   * @param queue
   * @param completeWatcher
   */
  def loadBlocksIntoQueue(hdms:Seq[HDMInfo], queue: BlockingQueue[AnyRef], completeWatcher:AtomicBoolean): Unit ={
    val blockCounter = new AtomicInteger(0)

    val blockHandler = (blk: Block[_]) => {
      if (blockCounter.incrementAndGet() >= hdms.length) {
        completeWatcher.set(true)
      }
      queue.offer(blk)
      log.info(s"Fetched block:${blk.id}, progress: (${blockCounter.get}/${hdms.length}).")
    }

    val fetchHandler = (resp: FetchSuccessResponse) => {
      if (blockCounter.incrementAndGet() >= hdms.length) {
        completeWatcher.set(true)
      }
      queue.offer(resp)
      log.info(s"Received fetch response:${resp.id} with size ${resp.length}, progress: (${blockCounter.get}/${hdms.length}).")
    }

    //group block by host address
    val blockByAddress = hdms.map(_.location).groupBy { p =>
      p.address
    }.map(bl => (Path(bl._1), bl._2.map(_.name)))
    //randomize the request to avoid IO contense
    val remoteBlocks = Utils.randomize(blockByAddress.toSeq)

    for (blocks <- remoteBlocks) {
      log.info(s"Fetching block from ${blocks._1} ...")
      HDMBlockManager.loadBlockAsync(blocks._1, blocks._2, blockHandler, fetchHandler)
    }

  }

  def loadOrDeclare[T: ClassTag](br:DDM[_,T]) :Block[T] = {
    val bl = defaultManager.getBlock(br.id)
    if(bl ne null) bl.asInstanceOf[Block[T]]
    else {
      if(defaultManager.getRef(br.id) == null)
        defaultManager.declare(br)
      Block(Seq.empty[T])
    }
  }

  def freeMemMB() = {
    val jvmMem = SystemMonitorService.getJVMMemInfo
    (jvmMem(2) - jvmMem(1) + jvmMem(0))/ (1024*1024F)
  }

  def cleanup(blk:Block[_]) = {
//    if(blk != null && blk.data != null)
//      blk.data.clear
  }

  def forceGC(): Unit = {
    val start = System.currentTimeMillis()
    val memBefore = HDMBlockManager.freeMemMB()
    System.gc()
    val end = System.currentTimeMillis() - start
    log.info(s"JVM GC took $end ms. Memory recycled:${HDMBlockManager.freeMemMB() - memBefore} MB.")
  }
}