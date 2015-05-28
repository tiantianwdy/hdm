package org.nicta.wdy.hdm.io.netty

import java.net.InetSocketAddress

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel._
import io.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import io.netty.handler.codec.string.StringDecoder
import io.netty.util.ReferenceCountUtil
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.{HDMBlockManager, BlockManager, Block}
import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockServer(val port:Int,
                       val nThreads:Int,
                       val blockManager: HDMBlockManager,
                       serializerInstance: SerializerInstance) extends Logging{

  private var f:ChannelFuture = _
  private var bt:ServerBootstrap = _
  private var bossGroup:EventLoopGroup = _
  private var workerGroup: EventLoopGroup = _

  def init(): Unit ={

    bossGroup = new NioEventLoopGroup(1)
    workerGroup = new NioEventLoopGroup(nThreads)
    try{
      bt = new ServerBootstrap()
      bt.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(c: SocketChannel): Unit = {
          c.pipeline()
            .addLast(new NettyBlockEncoder4x(serializerInstance))
            .addLast(new NettyQueryDecoder4x(serializerInstance))
//            .addLast(new StringDecoder)
//            .addLast(new ProtobufEncoder)
//            .addLast(new ProtobufDecoder)
            .addLast(new NettyBlockServerHandler(blockManager))
        }
      })
      .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

    } finally {

    }

  }

  def start(): Unit ={
    val addr = new InetSocketAddress(port)
    //bind and start
    f = bt.bind(addr).sync()
    log.info("Netty server is started at " + addr.getHostString + ":" + port)
//    f.channel().closeFuture().sync()
  }


  def shutdown(): Unit ={
    log.info(" Netty server is stopping ... ")
    f.channel().close().awaitUninterruptibly()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
    log.info(" Netty server is stopped successfully... ")
  }

}


class NettyBlockServerHandler(blockManager: HDMBlockManager) extends  ChannelInboundHandlerAdapter with Logging{

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    super.channelActive(ctx)
    log.info(" Connection activated:" + ctx)
  }


  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = try {
    log.info("received a message:" + msg)
    val query = msg.asInstanceOf[QueryBlockMsg]
    val blk = blockManager.getBlock(query.id)
    if(blk ne null){
      ctx.write(blk)
      ctx.flush()
    }
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




