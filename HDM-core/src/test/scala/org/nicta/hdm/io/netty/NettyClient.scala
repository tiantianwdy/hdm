package org.nicta.hdm.io.netty

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyBlockFetcher
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Block}

/**
 * Created by tiantian on 27/05/15.
 */
object NettyClient {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()

  def main(args:Array[String]): Unit ={
    val blkHandler = (blk:Block[_]) => {
      println(blk.id)
      blk.data.map(_.toString) foreach(println(_))
      HDMBlockManager().add(blk.id, blk)
    }
    val blockFetcher = new NettyBlockFetcher("0.0.0.0", 9091, serializer, blkHandler)
    blockFetcher.start()
    blockFetcher.sendRequest(QueryBlockMsg("blk-002",null))
    Thread.sleep(10000)
    val cachedBlk = HDMBlockManager().getBlock("blk-002")
    println(cachedBlk)
    blockFetcher.shutdown()
  }

}
