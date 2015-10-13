package org.nicta.wdy.hdm.io.netty

import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingDeque, Semaphore, TimeUnit}

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
import org.nicta.wdy.hdm.message.{FetchSuccessResponse, NettyFetchRequest, QueryBlockMsg}
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockFetcher( val serializerInstance: SerializerInstance) extends Logging{

  private var channel: Channel = _
  private var bt:Bootstrap = _
  private var workerGroup:EventLoopGroup = _
  private val allocator = NettyConnectionManager.createPooledByteBufAllocator(false, HDMContext.NETTY_BLOCK_CLIENT_THREADS)
  private val workingSize = new Semaphore(1)
  private val outGoingMsg = new AtomicInteger(0)
//  private val handler = new AtomicReference[Block[_] => Unit]()
  private val requestsQueue = new LinkedBlockingDeque[NettyFetchRequest]()
  private val running = new AtomicBoolean(false)
  private val callbackMap = new ConcurrentHashMap[String, FetchSuccessResponse => Unit]

  private val workingThread:Thread = new Thread {

    override def run(): Unit = {
      while(running.get()){
        workingSize.acquire(1)
        val blkIds = ArrayBuffer.empty[String]
        //merge multiple fetch messages into one
        do {
          val req = requestsQueue.take()
          req.msg.blockIds foreach {id =>
            callbackMap.put(id, req.callback)
          }
          blkIds ++= req.msg.blockIds
        } while(!requestsQueue.isEmpty)
        outGoingMsg.addAndGet(blkIds.length)
        val address = if (channel.remoteAddress() ne null ) channel.remoteAddress() else channel.localAddress()
        try{
          val msg = QueryBlockMsg(blkIds, address.toString)
          channel.writeAndFlush(msg).addListener(NettyChannelListener(channel, System.currentTimeMillis()))
//          Thread.sleep(100)
        } catch {
          case e =>
            log.error("send block request failed to address:" + address)
            channel.close()
            e.printStackTrace()
        }
      }
    }
  }

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
            .addLast("decoder", new NettyBlockByteDecoder4x(serializerInstance, HDMContext.getCompressor()))
            .addLast("handler", new NettyBlockFetcherHandler(outGoingMsg, workingSize, callbackMap))
        }
      })
        .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
        .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
        .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 120*1000)
        .option[ByteBufAllocator](ChannelOption.ALLOCATOR, allocator)
      //        .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 1024)
    } finally {
//      workerGroup.shutdownGracefully()
    }

  }

  def connect(host:String, port:Int): Unit ={
    // connect to server
    if(bt ne null){
      val cf = bt.connect(host,port)
      cf.awaitUninterruptibly(60*1000)
      channel = cf.channel()
    } else
      log.error("Netty bootstrap is not initiated!")
  }

  def schedule(): Unit ={
    running.set(true)
    workingThread.start()
  }
  
  def stopScheduling() = {
    running.set(false)
    workingThread.stop()
  }

  def sendRequest(msg:QueryBlockMsg, callback: FetchSuccessResponse => Unit ): Boolean ={
    requestsQueue.offer(NettyFetchRequest(msg, callback))
    true
  }

  def isConnected()={
    if(channel eq null) false
    else channel.isActive || channel.isOpen
  }

  def isRunning = running.get()

  def waitForClose() = {
    if(channel ne null) channel.closeFuture().sync()
  }


  def shutdown(): Unit = try {
    log.info("A netty client is shutting down...")
    stopScheduling()
  } finally {
    if(channel ne null) channel.close().sync()
    if(workerGroup ne null) workerGroup.shutdownGracefully()
    bt = null
  }

  def setHandler(msgId:String, handler: FetchSuccessResponse => Unit)  = this.callbackMap.put(msgId, handler)

}

/**
 * 
 * @param workingSize
 * @param callbackMap
 */
class NettyBlockFetcherHandler(val outgoingMsg: AtomicInteger, val workingSize:Semaphore, val callbackMap:ConcurrentHashMap[String, FetchSuccessResponse => Unit]) extends  ChannelInboundHandlerAdapter with Logging{

  override def channelActive(ctx: ChannelHandlerContext): Unit = super.channelActive(ctx)


  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = try {
    log.info("received a response:" + msg.getClass)
    msg match {
      case blk:Block[_] =>
        val callback = callbackMap.get(blk.id)
//        if(callback ne null) callback.apply(blk)
      case fetchResponse:FetchSuccessResponse =>
        val callback = callbackMap.get(fetchResponse.id)
        if(callback ne null) callback.apply(fetchResponse)
    }
  } catch {
    case e: Throwable =>  e.printStackTrace()
  } finally {
//    ReferenceCountUtil.release(msg)
    if(outgoingMsg.decrementAndGet() <= 0)
      workingSize.release(1)
  }


  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
//    log.error(cause.getCause.toString)
    ctx.close()
  }
}