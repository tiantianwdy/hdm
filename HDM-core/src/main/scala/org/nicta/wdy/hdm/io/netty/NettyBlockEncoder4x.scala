package org.nicta.wdy.hdm.io.netty

import java.util
import java.nio.ByteBuffer

import io.netty.buffer.{Unpooled, ByteBuf}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToByteEncoder, MessageToMessageEncoder}
import org.nicta.wdy.hdm.message.{QueryBlockMsg, HDMBlockMsg}

import org.nicta.wdy.hdm.serializer.SerializerInstance
import org.nicta.wdy.hdm.storage.Block

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

class NettyServerByteEncoder4x(serializerInstance: SerializerInstance) extends MessageToByteEncoder[Block[_]]{

  override def encode(ctx: ChannelHandlerContext, msg: Block[_], out: ByteBuf): Unit = {
    out.writeBytes(serializerInstance.serialize(msg).array())
  }
}

class NettyFetcherByteEncoder4x(serializerInstance: SerializerInstance) extends MessageToByteEncoder[QueryBlockMsg]{

  override def encode(ctx: ChannelHandlerContext, msg: QueryBlockMsg, out: ByteBuf): Unit = {
    out.writeBytes(serializerInstance.serialize(msg).array())
  }
}



class NettyBlockEncoder4x(serializerInstance: SerializerInstance) extends MessageToMessageEncoder[Block[_]]{

  override def encode(channelHandlerContext: ChannelHandlerContext, in: Block[_], list: util.List[AnyRef]): Unit = {
    val data = Unpooled.wrappedBuffer(serializerInstance.serialize(in))
    list.add(data)
  }

}

class NettyQueryEncoder4x(serializerInstance: SerializerInstance) extends MessageToMessageEncoder[HDMBlockMsg]{

  override def encode(ctx: ChannelHandlerContext, msg: HDMBlockMsg, out: util.List[AnyRef]): Unit = {
    val data = Unpooled.wrappedBuffer(serializerInstance.serialize(msg))
    out.add(data)
  }
}
