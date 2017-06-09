package org.hdm.core.storage


import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 4/06/15.
 */
class HDMBlockManagerTest {

  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  def createData = ArrayBuffer.fill[(String, Int)](1000000){
    ("test", 1)
  }

  @Test
  def testMemRecycle(): Unit ={
    val num = 20
    val blkIds = ArrayBuffer.empty[String]
    println(s"original mem size:${HDMBlockManager.freeMemMB()}")
    for (i <- 0 until num){
      println(s"==== create block $i...")
      val blk = Block(createData)
      HDMBlockManager().add(blk.id, blk)
      blkIds += blk.id
    }

    for(id <- blkIds){
      println(s"==== start recycle $id...")
      HDMBlockManager().removeBlock(id)
    }
//    HDMBlockManager.forceGC()
    Thread.sleep(1000)
  }

  @Test
  def testMemInstantRecycle(): Unit ={
    val num = 20
    val blkIds = ArrayBuffer.empty[String]
    println(s"original mem size:${HDMBlockManager.freeMemMB()}")
    for (i <- 0 until num){
      println(s"==== create block $i...")
      val blk = Block(createData)
      HDMBlockManager().add(blk.id, blk)
      blkIds += blk.id
      println(s"==== start recycle $i...")
      HDMBlockManager().removeBlock(blk.id)
    }

  }

}
