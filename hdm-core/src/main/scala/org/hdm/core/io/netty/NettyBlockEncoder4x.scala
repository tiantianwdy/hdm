package org.hdm.core.io.netty

import java.util

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToByteEncoder, MessageToMessageEncoder}
import org.hdm.core.io.CompressionCodec
import org.hdm.core.message.{HDMBlockMsg, QueryBlockMsg}
import org.hdm.core.serializer.SerializerInstance
import org.hdm.core.storage.Block
import org.hdm.core.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
/*class NettyBlockEncoder3x(serializerInstance: SerializerInstance) extends OneToOneEncoder{


  override def encode(channelHandlerContext: channel.ChannelHandlerContext, channel: Channel, obj: scala.Any): AnyRef = {
    if(obj.isInstanceOf[Block[_]]){
      serializerInstance.serialize(obj.asInstanceOf[Block[_]])
    } else {
      serializerInstance.serialize(obj)
    }
  }
}*/

class NettyBlockByteEncoder4x(serializerInstance: SerializerInstance, compressor:CompressionCodec) extends MessageToByteEncoder[Block[_]] with Logging{

  override def encode(ctx: ChannelHandlerContext, msg: Block[_], out: ByteBuf): Unit = {
    val start = System.currentTimeMillis()
    val data = if(compressor ne null) {
      compressor.compress(serializerInstance.serialize(msg).array())
    } else{
      serializerInstance.serialize(msg).array()
    }
//    val buf = ctx.alloc().heapBuffer(data.length)
    val end = System.currentTimeMillis() - start
    log.info(s"encoded data:${data.length} bytes, in $end ms.")
    out.writeInt(data.length + 4)
    out.writeBytes(data)
//    out.writeBytes(buf)
  }
}

class NettyBlockResponseEncoder4x(serializerInstance: SerializerInstance, compressor:CompressionCodec) extends MessageToByteEncoder[Block[_]] with Logging{

  override def encode(ctx: ChannelHandlerContext, msg: Block[_], out: ByteBuf): Unit = {
    val start = System.currentTimeMillis()
    val idData = serializerInstance.serialize(msg.id).array()
    val data = if(compressor ne null) {
      val compressed = compressor.compress(serializerInstance.serialize(msg).array())
      val end = System.currentTimeMillis() - start
      log.info(s"compressed data${compressed.length}, in $end ms.")
      compressed
    } else {
      val serialized = serializerInstance.serialize(msg).array()
      val end = System.currentTimeMillis() - start
      log.info(s"encoded data:${serialized.length} bytes, in $end ms.")
      serialized
    }
    //    val buf = ctx.alloc().heapBuffer(data.length)
    // data length + block Id length + length of this header
    out.writeInt(data.length + idData.length + 8)
    out.writeInt(idData.length)
    out.writeBytes(idData)
    out.writeBytes(data)
  }
}

class NettyQueryByteEncoder4x(serializerInstance: SerializerInstance) extends MessageToByteEncoder[QueryBlockMsg]{

  override def encode(ctx: ChannelHandlerContext, msg: QueryBlockMsg, out: ByteBuf): Unit = {
    out.writeBytes(serializerInstance.serialize(msg).array())
  }
}



class NettyBlockEncoder4x(serializerInstance: SerializerInstance) extends MessageToMessageEncoder[Block[_]]{

  override def encode(channelHandlerContext: ChannelHandlerContext, in: Block[_], list: util.List[AnyRef]): Unit = {

    val data = serializerInstance.serialize(in)
    val buf = Unpooled.wrappedBuffer(data)
    list.add(buf)
  }

}

class NettyQueryEncoder4x(serializerInstance: SerializerInstance) extends MessageToMessageEncoder[HDMBlockMsg]{

  override def encode(ctx: ChannelHandlerContext, msg: HDMBlockMsg, out: util.List[AnyRef]): Unit = {
    val data = Unpooled.wrappedBuffer(serializerInstance.serialize(msg))
    out.add(data)
  }
}


class NettyQueryFrameEncoder4x(serializerInstance: SerializerInstance) extends MessageToByteEncoder[HDMBlockMsg] with Logging{

  override def encode(ctx: ChannelHandlerContext, msg: HDMBlockMsg, out: ByteBuf): Unit = {
    val start = System.currentTimeMillis()
    val data = serializerInstance.serialize(msg).array()
    val end = System.currentTimeMillis() - start
    log.info(s"encoded data:${data.length} bytes, in $end ms.")
    out.writeInt(data.length  + 4)
    out.writeBytes(data)
  }
}