package org.hdm.core.io.netty

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBufAllocator
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.ReferenceCountUtil
import org.hdm.core.io.CompressionCodec
import org.hdm.core.message.QueryBlockMsg
import org.hdm.core.serializer.SerializerInstance
import org.hdm.core.storage.HDMBlockManager
import org.hdm.core.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockServer(val port:Int,
                       val nThreads:Int,
                       val blockManager: HDMBlockManager,
                       serializerInstance: SerializerInstance,
                       compressor:CompressionCodec) extends Logging{

  private var f:ChannelFuture = _
  private var bt:ServerBootstrap = _
  private var bossGroup:EventLoopGroup = _
  private var workerGroup: EventLoopGroup = _
  private val allocator = NettyConnectionManager.createPooledByteBufAllocator(true, nThreads)

  def init(): Unit ={

    bossGroup = new NioEventLoopGroup(nThreads)
    workerGroup = bossGroup
    try{
      bt = new ServerBootstrap()
      bt.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(c: SocketChannel): Unit = {
          c.pipeline()
          .addLast("encoder", new NettyBlockResponseEncoder4x(serializerInstance, compressor))
            .addLast("frameDecoder", NettyConnectionManager.getFrameDecoder())
            .addLast("decoder", new NettyQueryDecoder4x(serializerInstance))
            .addLast("handler", new NettyBlockServerHandler(blockManager))
//            .addLast(new StringDecoder)
//            .addLast(new ProtobufEncoder)
//            .addLast(new ProtobufDecoder)

        }
      })
        .option[ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)
        .childOption[ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)
//        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
//      .option[java.lang.Integer](ChannelOption.SO_SNDBUF, 1024)
//      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

    } finally {

    }

  }

  def start(): Unit ={
    val addr = new InetSocketAddress(NettyConnectionManager.localHost, port)
    //bind and start
    f = bt.bind(addr).syncUninterruptibly()
    log.info("Netty server is started at " + addr.getHostString + ":" + port)
  }


  def shutdown(): Unit ={
    log.info(" Netty server is stopping ... ")
    f.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
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
    query.blockIds.foreach{id =>
      val blk = blockManager.getBlock(id)
      if(blk ne null){
        while(!ctx.channel().isActive){
          log.warn(s"Reconnecting to ${ctx.channel().remoteAddress()}")
          ctx.connect(ctx.channel().remoteAddress()).awaitUninterruptibly(60 * 1000)
        }
        ctx.writeAndFlush(blk).addListener(NettyChannelListener(ctx.channel(), System.currentTimeMillis()))
      }
    }
//    ctx.flush()
  } catch {
    case e: Throwable => e.printStackTrace()
  } finally {
    ReferenceCountUtil.release(msg)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}




