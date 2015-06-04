package org.nicta.wdy.hdm.storage

import java.nio.ByteBuffer
import java.util.UUID
import io.netty.buffer.{Unpooled, ByteBuf}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.model.Buf
import org.nicta.wdy.hdm.serializer.SerializerInstance

import scala.reflect.ClassTag
import akka.serialization.{JavaSerializer, Serializer}
import org.nicta.wdy.hdm.io.DefaultJSerializer

/**
 * Created by Tiantian on 2014/12/1.
 */
trait Block[T] extends Serializable{

  val id : String

  def data: Buf[T]

  def size: Int
}

class UnserializedBlock[T](val id:String, val data:Buf[T]) extends  Block[T] {

  def size = data.size

}

class SerializedBlock[T <: Serializable] (val id:String, val elems:Buf[T])(implicit serializer:Serializer = new DefaultJSerializer) extends  Block[T] {

  private val block: Array[Byte] = serializer.toBinary(elems)

  def size = block.size

  def data = serializer.fromBinary(block).asInstanceOf[Buf[T]]

}


object Block {

  def apply[T](data:Buf[T]) = new UnserializedBlock[T](UUID.randomUUID().toString, data)

  def apply[T](id:String, data:Buf[T]) = new UnserializedBlock[T](id, data)

  def apply[T](data:Seq[T]) = new UnserializedBlock[T](UUID.randomUUID().toString, data.toBuffer)

  def apply[T](id:String, data:Seq[T]) = new UnserializedBlock[T](id, data.toBuffer)

  def sizeOf(obj:Any)(implicit serializer:Serializer = new DefaultJSerializer) : Int = obj match {
    case o:AnyRef =>
      serializer.toBinary(o).size
    case _ => 8 // primitive types are 8 bytes for 64-bit OS
  }

  def byteSize(blk:Block[_]):Int = blk match {
    case blk: UnserializedBlock[_] =>
      blk.size * sizeOf(if(blk.data.isEmpty) 0 else blk.data.head)
    case blk:SerializedBlock[_] =>
      blk.size
  }

  def encode(blk:Block[_])(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):ByteBuf = {
    val idBuf = serializer.serialize(blk.id).array()
    val dataBuf = serializer.serialize(blk.data).array()
    println(s"id length: ${idBuf.length}; default id length: ${HDMContext.DEFAULT_BLOCK_ID_LENGTH}")
    val length = idBuf.length + dataBuf.length + 4
    val byteBuf = Unpooled.buffer(length)
    byteBuf.writeInt(length)
    byteBuf.writeBytes(idBuf)
    byteBuf.writeBytes(dataBuf)
    byteBuf
  }

  def decode(buf:ByteBuf)(implicit serializer:SerializerInstance = HDMContext.defaultSerializer):Block[_] = {
    val length = buf.readInt() - 4 - HDMContext.DEFAULT_BLOCK_ID_LENGTH
    val idBuf = buf.nioBuffer(4, HDMContext.DEFAULT_BLOCK_ID_LENGTH)
    val dataBuf = buf.nioBuffer(4+HDMContext.DEFAULT_BLOCK_ID_LENGTH, length)
    val id = serializer.deserialize[String](idBuf)
    val data = serializer.deserialize[Buf[_]](dataBuf)
    new UnserializedBlock(id, data)
  }
}