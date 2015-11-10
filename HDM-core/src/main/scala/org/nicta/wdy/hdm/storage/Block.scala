package org.nicta.wdy.hdm.storage

import java.nio.ByteBuffer
import java.util.UUID
import _root_.io.netty.buffer.{Unpooled, ByteBuf}
import org.nicta.wdy.hdm.{Blk, Arr}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.message.FetchSuccessResponse
import org.nicta.wdy.hdm.serializer.SerializerInstance

import scala.reflect._
import akka.serialization.{JavaSerializer, Serializer}
import org.nicta.wdy.hdm.io.{CompressionCodec, DefaultJSerializer}

/**
 * Created by Tiantian on 2014/12/1.
 */
trait Block[T] extends Serializable{

  val id : String

  def data: Blk[T]

  def size: Int
}

class UnserializedBlock[T:ClassTag](val id:String, val data:Blk[T]) extends  Block[T] {

  def size = data.size

}

class SerializedBlock[T <: Serializable: ClassTag] (val id:String, val elems:Blk[T])(implicit serializer:Serializer = new DefaultJSerializer) extends  Block[T] {

  private val block: Array[Byte] = serializer.toBinary(elems)

  def size = block.size

  def data = serializer.fromBinary(block).asInstanceOf[Blk[T]]

}


object Block {

  def apply[T: ClassTag](data:Seq[T]) = new UnserializedBlock(UUID.randomUUID().toString, data.toBuffer)

  def apply[T: ClassTag](id:String, data:Seq[T]) = new UnserializedBlock(id, data.toBuffer)

  def apply[T: ClassTag](data:Arr[T]) = {
    new UnserializedBlock(UUID.randomUUID().toString, data.toBuffer)
  }

  def apply[T: ClassTag](id:String, data:Arr[T]) = new UnserializedBlock(id, data.toBuffer)

  def sizeOf(obj:Any)(implicit serializer:Serializer = new DefaultJSerializer) : Long = obj match {
    case o:AnyRef =>
      serializer.toBinary(o).size
    case _ => 8L // primitive types are 8 bytes for 64-bit OS
  }

  def byteSize(blk:Block[_]):Long = blk match {
    case blk: UnserializedBlock[_] =>
      blk.size * sizeOf(if(blk.data.isEmpty) 0L else blk.data.head)
    case blk:SerializedBlock[_] =>
      blk.size
  }

  def byteSize(elems:Seq[_]):Long = {
    if(elems == null || elems.isEmpty) 0
    else sizeOf(elems.head) * elems.size
  }

  def encodeToBuf(blk:Block[_])(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):ByteBuf = {
    val idBuf = serializer.serialize(blk.id).array()
    val dataBuf = serializer.serialize(blk).array()
    println(s"id length: ${idBuf.length}; default id length: ${HDMContext.DEFAULT_BLOCK_ID_LENGTH}")
    val length = idBuf.length + dataBuf.length + 8
    val byteBuf = Unpooled.buffer(length)
    byteBuf.writeInt(length)
    byteBuf.writeInt(idBuf.length)
    byteBuf.writeBytes(idBuf)
    byteBuf.writeBytes(dataBuf)
    byteBuf
  }

  def decodeBlock(buf:ByteBuf)(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):Block[_] = {
    val length = buf.readInt() - 4 - HDMContext.DEFAULT_BLOCK_ID_LENGTH
    val idBuf = buf.nioBuffer(4, HDMContext.DEFAULT_BLOCK_ID_LENGTH)
    val dataBuf = buf.nioBuffer(4+HDMContext.DEFAULT_BLOCK_ID_LENGTH, length)
    val id = serializer.deserialize[String](idBuf)
    val data = serializer.deserialize[Blk[_]](dataBuf)
    new UnserializedBlock(id, data)
  }

  def decodeResponse(buf:ByteBuf, compressor:CompressionCodec)(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):FetchSuccessResponse = {
    val length = buf.readableBytes()
    val idLen = buf.readInt()
    val (data, dataLen) = if(compressor ne null) {
      val bytes = new Array[Byte](length)
      buf.getBytes(0, bytes)
      val actualData = compressor.uncompress(bytes.drop(4 + idLen))
      (ByteBuffer.wrap(actualData), actualData.length)
    } else {
      val bufLen = length - idLen - 4
      val dataBuf = buf.nioBuffer(4 + idLen, bufLen)
      (dataBuf, bufLen)
    }
    val idBuf = buf.nioBuffer(4, idLen)
    val id = serializer.deserialize[String](idBuf)
    new FetchSuccessResponse(id, dataLen, data)
  }

//  def decodeBlock(buf:ByteBuffer)(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):Block[_] = {
//    serializer.deserialize[Block[_]](buf)
//  }
}