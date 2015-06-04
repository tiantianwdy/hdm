package org.nicta.hdm.io.netty

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.io.netty.{NettyConnectionManager, NettyBlockFetcher}
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Block}

/**
 * Created by tiantian on 27/05/15.
 */
object NettyClient {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()

  def main(args:Array[String]): Unit ={
    var blockFetcher = NettyConnectionManager.getInstance.getConnection("tiantian-HP-EliteBook-Folio-9470m", 9091)
    val blkHandler = (blk:Block[_]) => {
      println(s"received block ${blk.id} with size ${blk.data.length}.")
//      blk.data.map(_.toString) foreach(println(_))
      HDMBlockManager().add(blk.id, blk)
    }
//    val blockFetcher = new NettyBlockFetcher(serializer, blkHandler)
//    blockFetcher.init()
//    blockFetcher.connect("0.0.0.0", 9091)
    blockFetcher.sendRequest(QueryBlockMsg("blk-002","tiantian-HP-EliteBook-Folio-9470m:9091"), blkHandler)
//    Thread.sleep(100)
//    blockFetcher.waitForClose()
//    NettyConnectionManager.getInstance.recycleConnection("tiantian-HP-EliteBook-Folio-9470m", 9091, blockFetcher)
    blockFetcher = NettyConnectionManager.getInstance.getConnection("tiantian-HP-EliteBook-Folio-9470m", 9091)
    blockFetcher.sendRequest(QueryBlockMsg("blk-001","tiantian-HP-EliteBook-Folio-9470m:9091"), blkHandler)
//    blockFetcher.waitForClose()
    Thread.sleep(25000)
    val cachedBlk = HDMBlockManager().getBlock("blk-002")
    println(cachedBlk)
//    blockFetcher.shutdown()
  }

}
