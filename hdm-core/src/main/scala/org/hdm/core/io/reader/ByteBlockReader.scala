package org.hdm.core.io.reader

import java.io.{InputStream, OutputStream}


import scala.reflect.ClassTag

/**
  * Created by wu056 on 13/06/17.
  */
class ByteBlockReader extends  BlockReader{

  override def identifier: Int = 11

  override def includeManifest: Boolean = false

  override def fromBinary[T: ClassTag](bytes: Array[Byte], manifest: Option[Class[_]]): T = ???

  override def toBinary[T: ClassTag](o: T): Array[Byte] = ???

  override def fromInputStream[T: ClassTag](in: InputStream): Seq[T] = ???

  override def toOutputStream[T: ClassTag](lst: Seq[T], out: OutputStream): Unit = ???

  override def iteratorInputStream[T: ClassTag](in: InputStream): Iterator[T] = ???
}
