package org.nicta.wdy.hdm.io.netty

import java.net.{InetAddress, Inet4Address, InetSocketAddress}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.codec.{LengthFieldPrepender, MessageToByteEncoder, ByteToMessageDecoder, LengthFieldBasedFrameDecoder}
import io.netty.util.internal.PlatformDependent
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.utils.Logging
import scala.collection.JavaConversions._
import org.nicta.wdy.hdm.io.netty.NettyBlockFetcher
import org.nicta.wdy.hdm.storage.Block

/**
 * Created by tiantian on 31/05/15.
 */
class NettyConnectionManager {

  val connectionNumPerPeer = 4

  private val connectionCacheMap = new ConcurrentHashMap[String, ConnectionPool]()


/*  def getConnection(host:String, port:Int):NettyBlockFetcher ={
    val cId = host + ":" + port
    val activeCon = if(connectionCacheMap.containsKey(cId)) {
      val con = connectionCacheMap.get(cId)
      if(con.isConnected()) con
      else {
        con.shutdown()
        connectionCacheMap.remove(cId)
        NettyConnectionManager.createConnection(host, port)
      }
    } else {
      NettyConnectionManager.createConnection(host, port)
    }
    connectionCacheMap.put(cId, activeCon)
    if(!activeCon.isRunning) activeCon.schedule()
    activeCon
  }*/

  def getConnection(host:String, port:Int):NettyBlockFetcher ={
    val cId = host + ":" + port
    if(!connectionCacheMap.containsKey(cId)) {
      connectionCacheMap.put(cId, new ConnectionPool(connectionNumPerPeer,host, port))
    }
    val activeCon = connectionCacheMap.get(cId).getNext()
    if(!activeCon.isRunning) activeCon.schedule()
    activeCon
  }

  def recycleConnection(host:String, port:Int, con: NettyBlockFetcher) = {
    val cId = host + ":" + port
    if(!connectionCacheMap.contains(cId)){
      connectionCacheMap.put(cId, new ConnectionPool(connectionNumPerPeer,host, port))
    }
    connectionCacheMap.get(cId).recycle(con)
  }

  def clear(): Unit ={
    connectionCacheMap.values().foreach(_.dispose())
    connectionCacheMap.clear()
  }

}

object NettyConnectionManager extends Logging{

  lazy val defaultManager = new NettyConnectionManager

  val localHost = InetAddress.getLocalHost.getHostName

  def createPooledByteBufAllocator(allowDirectBuffers:Boolean) = {
    new PooledByteBufAllocator(allowDirectBuffers && PlatformDependent.directBufferPreferred())
  }

  def getFrameDecoder(): ChannelInboundHandlerAdapter = {
    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, -4, 4)
  }

/*  def getFrameEncoder():MessageToByteEncoder = {
    new LengthFieldPrepender(8)
  }*/

  def getPrivateStaticField(name:String) = try {
    val f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
    f.setAccessible(true);
    f.getInt(null);
  }

  def getInstance = defaultManager

  def createConnection(host:String, port:Int) = {
    log.info(s"creating a new connection for $host:$port")
    val blockFetcher = new NettyBlockFetcher(HDMContext.defaultSerializer)
    blockFetcher.init()
    blockFetcher.connect(host, port)
    blockFetcher
  }

}
