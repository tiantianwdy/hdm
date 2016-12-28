package org.hdm.core.storage

import org.hdm.core.model.ParHDM

import scala.reflect.ClassTag


/**
 * Created by Tiantian on 2014/12/1.
 */
trait BlockManager {

  def getBlockRefs(ids:Seq[String]): Seq[BlockRef]

  def findBlockRefs(idPattern: String => Boolean): Seq[BlockRef]

  def declare(br:ParHDM[_,_]): BlockRef

  def cache(id:String, bl: Block[_])

  def cacheAll(bm: Map[String, Block[_]])

  def getBlock(id:String):Block[_]

  def getBlockRef (id:String): BlockRef

  def addRef(br:BlockRef)

  def addAllRef(brs: Seq[BlockRef])

  def add(id:String, block:Block[_])

  def addAll(blocks:Map[String, Block[_]])

  def remove(id: String)

  def removeAll(id: Seq[String])

  def checkState(id:String, state:BlockState):Boolean

  def checkAllStates(ids:Seq[String], state:BlockState):Boolean
}

object BlockManager{

  lazy val defaultManager = new DefaultBlockManagerImpl // todo change to loading according to the config

  def apply():BlockManager = defaultManager

  def loadOrCompute[T:ClassTag](bID: String):Block[T] = {
    val bl = defaultManager.getBlock(bID)
    if(bl ne null) bl.asInstanceOf[Block[T]]
    else {
      //compute the block
//      if(defaultManager.getBlockRef(bID) != null)
//        defaultManager.addRef(bID)
      //todo change to submit a computing task
     Block(Seq.empty[T])
    }
  }

  def cache(bID: String, br:Block[_]) = ???
}
