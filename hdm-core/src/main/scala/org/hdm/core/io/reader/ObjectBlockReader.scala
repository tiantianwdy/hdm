package org.hdm.core.io.reader

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.hdm.core.serializer.SerializerInstance

import scala.reflect.ClassTag

/**
  * Created by wu056 on 13/06/17.
  */
class ObjectBlockReader(serializer:SerializerInstance, classLoader: ClassLoader) extends BlockReader{

  override def identifier: Int = 12

  override def includeManifest: Boolean = false

  override def fromBinary[T: ClassTag](bytes: Array[Byte], manifest: Option[Class[_]]): T = {
    val buf = ByteBuffer.wrap(bytes)
    serializer.deserialize(buf, classLoader)
  }

  override def toBinary[T: ClassTag](o: T): Array[Byte] = {
    serializer.serialize(o).array()
  }

  override def fromInputStream[T: ClassTag](in: InputStream): Seq[T] = {
   this.iteratorInputStream(in).toSeq
  }

  override def toOutputStream[T: ClassTag](lst: Seq[T], out: OutputStream): Unit = {
    serializer.serializeStream(out)
  }

  override def iteratorInputStream[T: ClassTag](in: InputStream): Iterator[T] = {
    serializer.deserializeStream(in).asIterator.asInstanceOf[Iterator[T]]
  }
}


object ObjectBlockReader {

  def apply(serializer: SerializerInstance, classLoader: ClassLoader): ObjectBlockReader = new ObjectBlockReader(serializer, classLoader)
}