package org.hdm.core.io.netty

import org.hdm.core.executor.HDMContext
import org.hdm.core.io.netty.NettyBlockServer
import org.hdm.core.serializer.JavaSerializer
import org.hdm.core.storage.{Block, HDMBlockManager}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
object NettyTestServer {

  val blkSize = 160*8

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()
  val compressor = HDMContext.defaultHDMContext.getCompressor()
  val blockServer = new NettyBlockServer(9091,
    4,
    HDMBlockManager(),
    serializer,
    compressor)


  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  val data2 = ArrayBuffer.fill[(String, List[Double])](10000){
    ("0xb601998146d35e06", List(1D))
  }


  def main(args:Array[String]): Unit = {
    HDMBlockManager.initBlockServer(HDMContext.defaultHDMContext)
    for(i <- 0 until blkSize){
      val id = s"blk-00$i"
      HDMBlockManager().add(id, Block(id, data2))
    }
  }

}
