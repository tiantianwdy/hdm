package org.nicta.wdy.hdm.storage

import java.util.UUID
import org.nicta.wdy.hdm.model.Buf

import scala.reflect.ClassTag
import akka.serialization.{JavaSerializer, Serializer}
import org.nicta.wdy.hdm.io.DefaultJSerializer

/**
 * Created by Tiantian on 2014/12/1.
 */
trait Block[T] extends Serializable{

  val id : String

  def data: Buf[T]

  val size: Int
}

class UnserializedBlock[T](val id:String, val data:Buf[T]) extends  Block[T] {

  //approximate data size of one element with this type T
  private lazy val metaDataSize = Block.sizeOf(if(data.isEmpty) 0 else data.head)

  lazy val size = data.size * metaDataSize

}

class SerializedBlock[T <: Serializable] (val id:String, val elems:Buf[T])(implicit serializer:Serializer = new DefaultJSerializer) extends  Block[T] {

  private val block: Array[Byte] = serializer.toBinary(elems)

  val size = block.size

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

}