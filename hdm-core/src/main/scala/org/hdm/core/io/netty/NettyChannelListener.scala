package org.hdm.core.io.netty

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}
import org.hdm.core.utils.Logging

/**
 * Created by tiantian on 12/06/15.
 */
case class NettyChannelListener(channel:Channel, startTime:Long) extends ChannelFutureListener with  Logging{

  override def operationComplete(future: ChannelFuture): Unit = {
    if(future.isSuccess){
      val end = System.currentTimeMillis() - startTime
      log.info(s"send msg to ${channel.remoteAddress().toString} succeeded in $end ms.")
    } else {
      log.error(s"send msg to ${channel.remoteAddress().toString} failed.", future.cause())
      channel.close()
    }
  }
}
