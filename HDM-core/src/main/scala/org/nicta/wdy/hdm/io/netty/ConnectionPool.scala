package org.nicta.wdy.hdm.io.netty

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CopyOnWriteArrayList, LinkedBlockingQueue, ArrayBlockingQueue}
import org.nicta.wdy.hdm.io.CompressionCodec
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.JavaConversions._

/**
 * Created by tiantian on 8/06/15.
 */
class ConnectionPool(val poolSize:Int, val host:String, val port:Int, threadsPerCon:Int, serializerInstance:SerializerInstance, compressor:CompressionCodec) extends Logging {

  val pool = new CopyOnWriteArrayList[NettyBlockFetcher]()
  val index = new AtomicInteger(0)

  def length() = pool.size()

  def getNext():NettyBlockFetcher = {
    val nextIdx = index.getAndIncrement % poolSize
    if(pool.length - 1 < nextIdx){
      //fill the pool on new index
      val con = NettyConnectionManager.createConnection(host, port, threadsPerCon, serializerInstance, compressor)
      pool.add(con)
      con
    } else {
      val con = pool.get(nextIdx)
      if(con.isConnected()) con
      else {
        con.shutdown()
        pool.remove(con)
        val nCon = NettyConnectionManager.createConnection(host, port, threadsPerCon, serializerInstance, compressor)
        pool.update(nextIdx, nCon)
        nCon
      }
    }
  }

  def recycle(con:NettyBlockFetcher): Unit ={
    if(con.isConnected()){
      if(pool.size() < poolSize)
        pool.add(con)
      else {
        pool.find(!_.isConnected()) match {
          case Some(cached) =>
            val idx = pool.indexOf(cached)
            cached.shutdown()
            pool.update(idx,con)
          case None => con.shutdown()
        }
      }
    } else {
      //for non-active connection, just shut down
      con.shutdown()
    }
  }

  def dispose(): Unit ={
    pool foreach(_.shutdown())
    pool.clear()
    index.set(0)
  }

}
