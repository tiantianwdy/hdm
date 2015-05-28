package org.nicta.hdm.io.netty

import org.junit.{After, Before,Test}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.{NettyBlockFetcher, NettyBlockServer}
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
class Netty4Test {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()
  val blockServer = new NettyBlockServer(9091,
    4,
    HDMBlockManager(),
    serializer)


  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  val data2 = ArrayBuffer.fill[(String, Int)](10){
    ("test", 1)
  }


  @Before
  def beforeTest: Unit ={
    HDMBlockManager().add("blk-001", Block("blk-001",data))
    HDMBlockManager().add("blk-002", Block("blk-002",data2))
    val thread = new Thread() {
      override def run(): Unit = {
        blockServer.init()
        blockServer.start()
      }
    }
    thread.setDaemon(true)
    thread.run()

  }

  @Test
  def testSendBlock(): Unit ={
    val blkHandler = (blk:Block[_]) => {
      println(blk.id)
      blk.data.map(_.toString) foreach(println(_))
      HDMBlockManager().add(blk.id, blk)
    }
    val blockFetcher = new NettyBlockFetcher(serializer, blkHandler)
    blockFetcher.init()
    blockFetcher.connect("127.0.0.1", 9091)
    blockFetcher.sendRequest(QueryBlockMsg("blk-002", null))
    blockFetcher.waitForClose()
    val cachedBlk = HDMBlockManager().getBlock("blk-002")
    println(cachedBlk)
    blockFetcher.shutdown()
  }



  @After
  def afterTest: Unit ={
    blockServer.shutdown()
  }

}
