package org.nicta.wdy.hdm.io.netty

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Semaphore, TimeUnit}

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel._
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder}
import io.netty.handler.codec.string.StringEncoder
import io.netty.util.ReferenceCountUtil
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block
import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockFetcher( val serializerInstance: SerializerInstance) extends Logging{

  private var f: Channel = _
  private var bt:Bootstrap = _
  private var workerGroup:EventLoopGroup = _
  private val allocator = NettyConnectionManager.createPooledByteBufAllocator(true)
  private val workingSize = new Semaphore(1)
  private val handler = new AtomicReference[Block[_] => Unit]()

  def init(): Unit ={
    workerGroup = new NioEventLoopGroup(HDMContext.NETTY_BLOCK_CLIENT_THREADS)

    try{
      bt = new Bootstrap()
      bt.group(workerGroup)
      bt.channel(classOf[NioSocketChannel])
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(c: SocketChannel): Unit = {
          c.pipeline()
//            .addLast(new StringEncoder)
            .addLast("encoder", new NettyQueryEncoder4x(serializerInstance))
            .addLast("frameDecoder", NettyConnectionManager.getFrameDecoder())
            .addLast("decoder", new NettyBlockDecoder4x(serializerInstance))
            //            .addLast(new ProtobufEncoder)
//            .addLast(new ProtobufDecoder)
            .addLast("handler", new NettyBlockFetcherHandler(workingSize, handler))
        }
      })
        .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
        .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
        .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 120*1000)
        .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 1024)
        .option[ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)


    } finally {
//      workerGroup.shutdownGracefully()
    }

  }

  def connect(host:String, port:Int): Unit ={
    // connect to server
    if(bt ne null){
      val cf = bt.connect(host,port)
      cf.awaitUninterruptibly(60*1000)
      f = cf.channel()
    } else
      log.error("Netty bootstrap is not initiated!")
  }

  def sendRequest(msg:Any, blockHandler: Block[_] => Unit ): Boolean ={
    workingSize.acquire(1)
    handler.set(blockHandler)
    val success = f.writeAndFlush(msg).awaitUninterruptibly(60,TimeUnit.SECONDS)
    success
  }

  def isConnected()={
    if(f eq null) false
    else f.isActive || f.isOpen
  }

  def waitForClose() = {
    if(f ne null) f.closeFuture().sync()
  }


  def shutdown(): Unit ={
    log.info("A netty client is shutting down...")
    if(f ne null) f.close().sync()
    if(workerGroup ne null) workerGroup.shutdownGracefully()
    bt = null
  }

  def setHandler(handler: Block[_] => Unit)  = this.handler.set(handler)

}


class NettyBlockFetcherHandler(val workingSize:Semaphore, val blockHandler: AtomicReference[Block[_] => Unit]) extends  ChannelInboundHandlerAdapter with Logging{

  override def channelActive(ctx: ChannelHandlerContext): Unit = super.channelActive(ctx)


  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = try {
    log.info("received a response:" + msg.getClass)
    val blk = msg.asInstanceOf[Block[_]]
    blockHandler.get().apply(blk)
  } catch {
    case e: Throwable =>  e.printStackTrace()
  } finally {
    ReferenceCountUtil.release(msg)
    workingSize.release(1)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
//    log.error(cause.getCause.toString)
    ctx.close()
  }
}