package org.nicta.wdy.hdm.io.netty

import java.net.{InetAddress, Inet4Address, InetSocketAddress}
import java.util.concurrent.ConcurrentHashMap

import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.handler.codec.{LengthFieldPrepender, MessageToByteEncoder, ByteToMessageDecoder, LengthFieldBasedFrameDecoder}
import io.netty.util.internal.PlatformDependent
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyBlockFetcher
import org.nicta.wdy.hdm.storage.Block

/**
 * Created by tiantian on 31/05/15.
 */
class NettyConnectionManager {

  private val connectionCacheMap = new ConcurrentHashMap[String, NettyBlockFetcher]()

  private def createConnection(host:String, port:Int) = {
    val blockFetcher = new NettyBlockFetcher(HDMContext.defaultSerializer)
    blockFetcher.init()
    blockFetcher.connect(host, port)
    blockFetcher
  }

  def getConnection(host:String, port:Int):NettyBlockFetcher ={
    val cId = host + ":" + port
    val activeCon = if(connectionCacheMap.containsKey(cId)) {
      val con = connectionCacheMap.get(cId)
      if(con.isConnected()) con
      else {
        con.shutdown()
        connectionCacheMap.remove(cId)
        createConnection(host, port)
      }
    } else {
      createConnection(host, port)
    }
    connectionCacheMap.put(cId, activeCon)
    if(!activeCon.isRunning) activeCon.schedule()
    activeCon
  }

  def recycleConnection(host:String, port:Int, con: NettyBlockFetcher) = {
    val cId = host + ":" + port
    if(!connectionCacheMap.contains(cId) && con.isConnected())
      connectionCacheMap.put(cId, con)
    else if(connectionCacheMap.contains(cId)){
      val oldCon = connectionCacheMap.get(cId)
      if(oldCon.isConnected())
        con.shutdown()
      else{
        oldCon.shutdown()
        connectionCacheMap.put(cId, con)
      }
    } else {
      con.shutdown()
    }
  }

}

object NettyConnectionManager {

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

}
