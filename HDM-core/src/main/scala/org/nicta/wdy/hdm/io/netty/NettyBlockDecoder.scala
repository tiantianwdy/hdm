package org.nicta.wdy.hdm.io.netty

import java.nio.ByteBuffer
import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToMessageDecoder}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.nicta.wdy.hdm.message.QueryBlockMsg
import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block

/**
 * Created by tiantian on 27/05/15.
 */
class NettyBlockDecoder3x(serilizer: SerializerInstance) extends FrameDecoder{

  override def decode(channelHandlerContext: ChannelHandlerContext, channel: Channel, channelBuffer: ChannelBuffer): AnyRef = {
   val buf = channelBuffer.toByteBuffer
    serilizer.deserialize[Block[_]](buf)
  }
}

class NettyByteBlockDecoder4x(serilizer: SerializerInstance) extends ByteToMessageDecoder{

  override def decode(ctx: channel.ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    val obj = serilizer.deserialize(in.nioBuffer())
    out.add(obj)
  }
}

class NettyBlockDecoder4x(serilizer: SerializerInstance) extends MessageToMessageDecoder[ByteBuf]{

  override def decode(ctx: channel.ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {
    println("deserializing msg:" + msg)
    val obj = serilizer.deserialize[Block[_]](msg.nioBuffer())
    println("deserialized msg:" + obj)
    out.add(obj)
  }
}

class NettyQueryDecoder4x(serilizer: SerializerInstance) extends MessageToMessageDecoder[ByteBuf]{

  override def decode(ctx: channel.ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {
    println("deserializing msg:" + msg)
    val obj = serilizer.deserialize[QueryBlockMsg](msg.nioBuffer())
    println("deserialized msg:" + obj)
    out.add(obj)
  }
}


