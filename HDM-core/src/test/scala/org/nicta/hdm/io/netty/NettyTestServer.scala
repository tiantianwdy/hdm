package org.nicta.hdm.io.netty

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyBlockServer
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
object NettyTestServer {

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

  val data2 = ArrayBuffer.fill[(String, List[Double])](1000000){
    ("0xb601998146d35e06", List(1D))
  }


  def main(args:Array[String]): Unit ={
    HDMBlockManager.initBlockServer()
    HDMBlockManager().add("blk-001", Block("blk-001",data))
    HDMBlockManager().add("blk-002", Block("blk-002",data2))
//    blockServer.init()
//    blockServer.start()

  }
}
