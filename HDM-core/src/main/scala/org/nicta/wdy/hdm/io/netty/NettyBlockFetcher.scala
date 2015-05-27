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
class NettyBlockFetcher( val host:String,
                         val port:Int,
                         val serializerInstance: SerializerInstance,
                         val blockHandler: Block[_] => Unit ) {

  private var f: Channel = _

  def start(): Unit ={
    val workerGroup = new NioEventLoopGroup(2)

    try{
      val bt = new Bootstrap()
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
      // connect to server
      f = bt.connect(host,port).sync().channel()

    } finally {
//      workerGroup.shutdownGracefully()
    }

  }

  def sendRequest(msg:Any): Unit ={
    val success = f.writeAndFlush(msg).await(60, TimeUnit.SECONDS)
    println("send block query success:" + success)
    //wait until closed
//    f.closeFuture().sync()
  }


  def shutdown(): Unit ={
    f.close().sync()
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