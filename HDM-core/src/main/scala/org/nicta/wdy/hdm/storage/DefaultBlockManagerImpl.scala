package org.nicta.wdy.hdm.storage

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.UUID

import scala.collection.JavaConversions._

import org.nicta.wdy.hdm.model.{DFM, DDM, HDM}
/**
 * Created by Tiantian on 2014/12/8.
 */
class DefaultBlockManagerImpl extends BlockManager{

  val blockCache = new ConcurrentHashMap[String, Block[_]]()

  val blockRefMap = new ConcurrentHashMap[String, BlockRef]()

  override def removeAll(ids: Seq[String]): Unit = {
      ids.foreach{s =>
        blockCache.remove(s)
        blockRefMap.remove(s)
      }
  }

  override def remove(id: String): Unit = {
    blockCache.remove(id)
    blockRefMap.remove(id)
  }

  override def addAll(blocks: Map[String, Block[_]]): Unit = {
     blocks.foreach(b => add(b._1, b._2))
  }

  override def add(id: String, block: Block[_]): Unit = {
    if(blockRefMap.contains(id)){
      blockCache.put(id, block)
    } else throw new Exception("block has not been declared.")
  }

  override def addAllRef(brs: Seq[BlockRef]): Unit = {
    brs.foreach(addRef(_))
  }

  override def addRef(br: BlockRef): Unit = {
    if(blockRefMap.contains(br.id)) //override will clean the cache
      blockCache.remove(br.id)
    blockRefMap.put(br.id, br)
  }


  override def cacheAll(bm: Map[String, Block[_]]): Unit = addAll(bm)

  override def cache(id: String, bl: Block[_]): Unit = add(id, bl)



  override def getBlockRef(id: String): BlockRef = blockRefMap.get(id)

  override def getBlock(id: String): Block[_] = blockCache.get(id)


  override def declare(br: HDM[_, _]): BlockRef = {
    val (tp, blocks) = br match {
      case d:DDM[_,_] => ("DDM", Some(d))
      case d:DFM[_,_] => ("DFM", None)
      case _ => ("", None)
    }
    val ref = BlockRef(id = UUID.randomUUID().toString, typ = tp, parents = br.children.map(_.id),distribution = br.distribution, funcDepcy = br.func, state = Declared )
    blocks match {
      case Some(elems) =>
//        blockCache.put(br.id, Block(elems))
        blockRefMap.put(br.id, ref.copy(state = Computed))
      case None  =>
        blockRefMap.put(br.id, ref)
    }
  }

  override def findBlockRefs(idPattern: (String) => Boolean): Seq[BlockRef] = {
    getBlockRefs(blockRefMap.keySet().filter(idPattern).toSeq)
  }

  override def getBlockRefs(ids: Seq[String]): Seq[BlockRef] = {
    ids.map(id => blockRefMap.get(id)).filter(b => b ne null)
  }

  override def checkAllStates(ids: Seq[String], state: BlockState): Boolean = {
    ids.forall(checkState(_, state))
  }

  override def checkState(id: String, state: BlockState): Boolean = {
    blockRefMap.get(id).state == state
  }
}
