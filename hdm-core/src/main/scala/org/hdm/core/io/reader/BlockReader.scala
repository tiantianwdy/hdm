package org.hdm.core.io.reader

import java.io.{InputStream, OutputStream, Serializable}

import scala.reflect.ClassTag

/**
  * Created by wu056 on 13/06/17.
  */
trait BlockReader extends  Serializable {

  def identifier: Int

  def includeManifest: Boolean

  def fromBinary[T](bytes: Array[Byte], manifest: Option[Class[_]])(implicit tag:ClassTag[T]): T

  def toBinary[T](o: T)(implicit tag:ClassTag[T]): Array[Byte]

  def fromInputStream[T](in: InputStream)(implicit tag:ClassTag[T]): Seq[T]

  def toOutputStream[T](lst: Seq[T], out: OutputStream)(implicit tag:ClassTag[T]): Unit

  def iteratorInputStream[T](in: InputStream)(implicit tag:ClassTag[T]): Iterator[T]

}
