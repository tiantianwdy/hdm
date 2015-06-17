package org.nicta.wdy.hdm.io.netty

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}
import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by tiantian on 12/06/15.
 */
case class NettyChannelListener(channel:Channel, startTime:Long) extends ChannelFutureListener with  Logging{

  override def operationComplete(future: ChannelFuture): Unit = {
    if(future.isSuccess){
      val end = System.currentTimeMillis() - startTime
      log.info(s"send msg to ${channel.remoteAddress().toString} successed in $end ms.")
    } else {
      log.error(s"send msg to ${channel.remoteAddress().toString} failed.")
      channel.close()
    }
  }
}
