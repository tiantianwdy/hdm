package org.hdm.core.io.netty

import org.hdm.core.executor.HDMContext
import org.hdm.core.message.{FetchSuccessResponse, QueryBlockMsg}
import org.hdm.core.serializer.JavaSerializer
import org.hdm.core.storage.{Block, HDMBlockManager}

/**
 * Created by tiantian on 27/05/15.
 */
object NettyClient {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()

  def main(args:Array[String]): Unit ={

//    val blkHandler = (blk:Block[_]) => {
//      println(s"received block ${blk.id} with size ${blk.data.length}.")
////      blk.data.map(_.toString) foreach(println(_))
//      HDMBlockManager().add(blk.id, blk)
//    }
    val blkHandler = (resp:FetchSuccessResponse) => {
      println(s"received block ${resp.id} with size ${resp.length}.")
      val blk = serializer.deserialize[Block[_]](resp.data)
      println(blk.data.length)
//      HDMBlockManager().add(blk.id, blk)
    }
    var blockFetcher = NettyConnectionManager.getInstance.getConnection("tiantian-HP-EliteBook-Folio-9470m", 9091)
    val success = blockFetcher.sendRequest(QueryBlockMsg(Seq("blk-002","blk-003"),"tiantian-HP-EliteBook-Folio-9470m:9091"), blkHandler)
//    blockFetcher.waitForClose()
//    NettyConnectionManager.getInstance.recycleConnection("tiantian-HP-EliteBook-Folio-9470m", 9091, blockFetcher)
    blockFetcher = NettyConnectionManager.getInstance.getConnection("tiantian-HP-EliteBook-Folio-9470m", 9091)
    blockFetcher.sendRequest(QueryBlockMsg(Seq("blk-001"),"tiantian-HP-EliteBook-Folio-9470m:9091"), blkHandler)

    blockFetcher = NettyConnectionManager.getInstance.getConnection("tiantian-HP-EliteBook-Folio-9470m", 9091)
    blockFetcher.sendRequest(QueryBlockMsg(Seq("blk-002"),"tiantian-HP-EliteBook-Folio-9470m:9091"), blkHandler)
//    blockFetcher.waitForClose()
    Thread.sleep(250000)
    val cachedBlk = HDMBlockManager().getBlock("blk-002")
    println(cachedBlk)
//    NettyConnectionManager.getInstance.clear()
  }

}
