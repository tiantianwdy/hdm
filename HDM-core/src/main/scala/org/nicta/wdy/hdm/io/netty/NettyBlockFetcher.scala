package org.nicta.wdy.hdm.io.netty

import java.util.concurrent.TimeUnit

import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel._
import io.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import io.netty.handler.codec.string.StringEncoder
import io.netty.util.ReferenceCountUtil
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block
import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockFetcher( val serializerInstance: SerializerInstance,
                         val blockHandler: Block[_] => Unit ) extends Logging{

  private var f: Channel = _
  private var bt:Bootstrap = _
  private var workerGroup:EventLoopGroup = _

  def init(): Unit ={
    workerGroup = new NioEventLoopGroup(2)

    try{
      bt = new Bootstrap()
      bt.group(workerGroup)
      bt.channel(classOf[NioSocketChannel])
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(c: SocketChannel): Unit = {
          c.pipeline()
//            .addLast(new StringEncoder)
            .addLast(new NettyQueryEncoder4x(serializerInstance))
            .addLast(new NettyBlockDecoder4x(serializerInstance))
//            .addLast(new ProtobufEncoder)
//            .addLast(new ProtobufDecoder)
            .addLast(new NettyBlockFetcherHandler(blockHandler))
        }
      })
        .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)


    } finally {
//      workerGroup.shutdownGracefully()
    }

  }

  def connect(host:String, port:Int): Unit ={
    // connect to server
    if(bt ne null)
      f = bt.connect(host,port).sync().channel()
    else
      log.error("Netty bootstrap is not initiated!")
  }

  def sendRequest(msg:Any): Boolean ={
    val success = f.writeAndFlush(msg).await(60, TimeUnit.SECONDS)
    log.debug("send block query success:" + success)
    //wait until closed
    success
  }

  def isConnected()={
    if(f eq null) false
    else f.isActive
  }

  def waitForClose() = {
    if(f ne null) f.closeFuture().sync()
  }


  def shutdown(): Unit ={
    if(f ne null) f.close().sync()
    if(workerGroup ne null) workerGroup.shutdownGracefully()
    bt = null
  }

}


class NettyBlockFetcherHandler(blockHandler: Block[_] => Unit) extends  ChannelInboundHandlerAdapter with Logging{

  override def channelActive(ctx: ChannelHandlerContext): Unit = super.channelActive(ctx)


  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = try {
    log.info("received a response:" + msg)
    val blk = msg.asInstanceOf[Block[_]]
    blockHandler.apply(blk)
  } catch {
    case e: Throwable => log.error(e.getCause.toString)
  } finally {
    ReferenceCountUtil.release(msg)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.error(cause.getCause.toString)
    ctx.close()
  }
}