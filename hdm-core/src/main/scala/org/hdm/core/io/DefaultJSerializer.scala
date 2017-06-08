package org.hdm.core.io

import akka.serialization.{JavaSerializer, Serializer}
import java.io.{ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import akka.util.ClassLoaderObjectInputStream

/**
 * Created by Tiantian on 2014/12/9.
 * Default Java Serializer
 */
class DefaultJSerializer(val classLoader:ClassLoader = ClassLoader.getSystemClassLoader) extends  Serializer{

  def includeManifest: Boolean = false

  def identifier = 1

  def toBinary(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(o)
    out.close()
    bos.toByteArray
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val in = new ClassLoaderObjectInputStream(classLoader, new ByteArrayInputStream(bytes))
    val obj = in.readObject
    in.close()
    obj
  }
}
