package org.nicta.wdy.hdm.storage

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.{NettyBlockServer, NettyConnectionManager, NettyBlockFetcher}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.model.{DFM, HDM, DDM}
import java.util.concurrent.ConcurrentHashMap

import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by Tiantian on 2014/12/15.
 */
trait HDMBlockManager {

  def getRefs(ids:Seq[String]): Seq[HDM[_,_]]

  def findRefs(idPattern: String => Boolean): Seq[HDM[_,_]]

  def declare(br:HDM[_,_]): HDM[_,_]

  def cache(id:String, bl: Block[_])

  def cacheAll(bm: Map[String, Block[_]])

  def getBlock(id:String):Block[_]

  def getRef (id:String): HDM[_,_]

  def addRef(br:HDM[_,_])

  def addAllRef(brs: Seq[HDM[_,_]])

  def add(id:String, block:Block[_])

  def addAll(blocks:Map[String, Block[_]])

  def addAll(blocks:Seq[Block[_]])

  def removeRef(id: String)

  def removeBlock(id:String)

  def removeAll(id: Seq[String])

  def isCached(id:String):Boolean = ???

  def checkState(id:String, state:BlockState):Boolean

  def checkAllStates(ids:Seq[String], state:BlockState):Boolean

}


class DefaultHDMBlockManager extends HDMBlockManager{

  import scala.collection.JavaConversions._

  val blockCache = new ConcurrentHashMap[String, Block[_]]()

  val blockRefMap = new ConcurrentHashMap[String, HDM[_,_]]()


  override def checkAllStates(ids: Seq[String], state: BlockState): Boolean = {
    getRefs(ids).forall(_.state == state)
  }

  override def checkState(id: String, state: BlockState): Boolean = {
    getRef(id).state == state
  }

  override def removeAll(ids: Seq[String]): Unit = {
    ids.foreach(removeRef(_))
  }

  override def removeRef(id: String): Unit = {
    blockCache.remove(id)
    blockRefMap.remove(id)
  }


  override def removeBlock(id: String): Unit = {
    blockCache.remove(id)
  }

  override def addAll(blocks: Map[String, Block[_]]): Unit = {
    blockCache.putAll(blocks)
  }

  override def addAll(blocks: Seq[Block[_]]): Unit = {
    blocks.foreach(br => add(br.id, br))
  }

  override def add(id: String, block: Block[_]): Unit = {
    blockCache.put(id, block)
  }

  override def addAllRef(brs: Seq[HDM[_, _]]): Unit = {
    brs.foreach(addRef(_))
  }

  override def addRef(br: HDM[_, _]): Unit = {
    blockRefMap.put(br.id, br)
  }

  override def getRef(id: String): HDM[_, _] = {
    blockRefMap.get(id)
  }

  override def getBlock(id: String): Block[_] = {
    blockCache.get(id)
  }

  override def cacheAll(bm: Map[String, Block[_]]): Unit = addAll(bm)

  override def cache(id: String, bl: Block[_]): Unit = add(id, bl)

  override def declare(br: HDM[_, _]): HDM[_, _] = {
    addRef(br)
    br
  }

  override def findRefs(idPattern: (String) => Boolean): Seq[HDM[_, _]] = {
    getRefs(blockRefMap.keySet().filter(idPattern).toSeq)
  }

  override def getRefs(ids: Seq[String]): Seq[HDM[_, _]] = {
    ids.map(blockRefMap.get(_))
  }

  override def isCached(id: String): Boolean = {
    blockCache.containsKey(id)
  }
}

object HDMBlockManager extends Logging{

  lazy val defaultManager = new DefaultHDMBlockManager // todo change to loading according to the config

  lazy val defaultBlockServer =  new NettyBlockServer(HDMContext.NETTY_BLOCK_SERVER_PORT,
    HDMContext.NETTY_BLOCK_SERVER_THREADS,
    defaultManager,
    HDMContext.defaultSerializer)

  def initBlockServer() = {
    defaultBlockServer.init()
    defaultBlockServer.start()
  }

  def shutdown(): Unit ={
    defaultBlockServer.shutdown()
  }

  def apply():HDMBlockManager = defaultManager

  def loadOrCompute[T](refID: String):Seq[Block[T]] = {
    val id = Path(refID).name
    val ref = defaultManager.getRef(id)
    if (ref != null && ref.state == Computed)
      ref.blocks.map(url => loadBlock[T](url))
    else Seq.empty[Block[T]]
  }

  def loadBlock[T](url:String):Block[T] ={
    val path = Path(url)
//    val bl = defaultManager.getBlock(id)*/
    val bl = DataParser.readBlock(path)
    if(bl ne null) bl.asInstanceOf[Block[T]]
    else {
      //compute the block
      //todo change to submit a computing task
      Block(Seq.empty[T])
    }
  }

  def loadBlockAsync(path:Path, blockHandler: Block[_] => Unit) ={
    if (defaultManager.isCached(path.name)){
      log.info(s"block:${path.name} is at local")
      blockHandler.apply(defaultManager.getBlock(path.name))
    } else { // change to support of different protocol
      val blockFetcher = NettyConnectionManager.getInstance.getConnection(path.host, path.port)
      //
      val success = blockFetcher.sendRequest(QueryBlockMsg(path.name, path.host + ":" + path.port), blockHandler)
      if(!success) throw new RuntimeException("send block request failed to path:" + path)
//      NettyConnectionManager.getInstance.recycleConnection(path.host, path.port, blockFetcher)
    }
  }

  def loadOrDeclare[T](br:DDM[_,T]) :Block[T] = {
    val bl = defaultManager.getBlock(br.id)
    if(bl ne null) bl.asInstanceOf[Block[T]]
    else {
      if(defaultManager.getRef(br.id) == null)
        defaultManager.declare(br)
      Block(Seq.empty[T])
    }
  }
}