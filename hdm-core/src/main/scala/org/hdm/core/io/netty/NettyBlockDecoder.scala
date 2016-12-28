package org.hdm.core.io.netty

import java.nio.ByteBuffer
import java.util

import io.netty.buffer.{ByteBufInputStream, ByteBuf}
import io.netty.channel
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToMessageDecoder}
import io.netty.util.ReferenceCountUtil
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.hdm.core.io.CompressionCodec
import org.hdm.core.message.{FetchSuccessResponse, QueryBlockMsg}
import org.hdm.core.serializer.SerializerInstance
import org.hdm.core.storage.Block
import org.hdm.core.utils.Logging

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockDecoder3x(serializer: SerializerInstance) extends FrameDecoder{

  override def decode(channelHandlerContext: ChannelHandlerContext, channel: Channel, channelBuffer: ChannelBuffer): AnyRef = {
   val buf = channelBuffer.toByteBuffer
    serializer.deserialize[Block[_]](buf)
  }
}

class NettyBlockByteDecoder4x(serializer: SerializerInstance, compressor:CompressionCodec) extends MessageToMessageDecoder[ByteBuf]  with Logging{

  override def decode(ctx: channel.ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = try {
    log.debug("de-serializing msg:" + msg)
    log.debug("current decoding msg size:" + msg.readableBytes())
    val start = System.currentTimeMillis()
    val obj = Block.decodeResponse(msg.copy(), compressor)
    val end = System.currentTimeMillis() - start
    log.info(s"de-serialized data:${obj.length} bytes in $end ms.")
    out.add(obj)
  } finally {
    msg.clear()
    //      ReferenceCountUtil.release(msg)
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
    log.info("de-serializing msg:" + msg)
    val obj = serializer.deserialize[QueryBlockMsg](msg.nioBuffer())
    log.info("de-serialized msg:" + obj)
    out.add(obj)
  }
}


