package org.nicta.wdy.hdm.io.netty

import java.nio.ByteBuffer
import java.util

import io.netty.buffer.{ByteBufInputStream, ByteBuf}
import io.netty.channel
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToMessageDecoder}
import io.netty.util.ReferenceCountUtil
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.nicta.wdy.hdm.io.CompressionCodec
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block
import org.nicta.wdy.hdm.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockDecoder3x(serializer: SerializerInstance) extends FrameDecoder{

  override def decode(channelHandlerContext: ChannelHandlerContext, channel: Channel, channelBuffer: ChannelBuffer): AnyRef = {
   val buf = channelBuffer.toByteBuffer
    serializer.deserialize[Block[_]](buf)
  }
}

class NettyBlockByteDecoder4x(serializer: SerializerInstance) extends ByteToMessageDecoder with Logging{

  override def decode(ctx: channel.ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    if(in.readableBytes() < 4) return
    val dataLength = in.getInt(in.readerIndex())
    in.retain()
    if(in.readableBytes() >= dataLength) try {
      log.debug("expected decoding msg size:" + dataLength)
      log.debug("current decoding msg size:" + in.readableBytes())
      val obj = serializer.deserialize[Block[_]](in.nioBuffer(4, dataLength))
      out.add(obj)
    } finally {
      ReferenceCountUtil.release(in)
      in.clear()
    }
  }
}

class NettyBlockDecoder4x(serializer: SerializerInstance, compressor:CompressionCodec) extends MessageToMessageDecoder[ByteBuf] with Logging{

  override def decode(ctx: channel.ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = try {
    log.debug("de-serializing msg:" + msg)
//    val len = msg.readInt()
    log.debug("current decoding msg size:" + msg.readableBytes())
    val start = System.currentTimeMillis()
    val buf = if(compressor ne null) {
      val bytes = new Array[Byte](msg.readableBytes())
      msg.readBytes(bytes)
      ByteBuffer.wrap(compressor.uncompress(bytes))
    } else msg.nioBuffer()
    val obj = serializer.deserialize[Block[_]](buf)
    val end = System.currentTimeMillis() - start
    log.info(s"de-serialized data:${obj.data.size} in $end ms.")
    out.add(obj)
  } finally {
    msg.clear()
  }
}

class NettyQueryDecoder4x(serializer: SerializerInstance) extends MessageToMessageDecoder[ByteBuf] with Logging{

  override def decode(ctx: channel.ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {
    log.debug("de-serializing msg:" + msg)
    val obj = serializer.deserialize[QueryBlockMsg](msg.nioBuffer())
    log.debug("de-serialized msg:" + obj)
    out.add(obj)
  }
}


