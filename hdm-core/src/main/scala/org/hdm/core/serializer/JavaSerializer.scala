/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hdm.core.serializer

import java.io._
import java.nio.ByteBuffer

import com.typesafe.config.Config
import org.hdm.core.utils.DynamicDependencyThreadFactory

import scala.reflect.ClassTag

import org.hdm.core.io.ByteBufferInputStream

/**
 * edited from org.apache.spark.serializer.JavaSerializer
 */

private[hdm] class JavaSerializationStream(
    out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
//        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[hdm] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
extends DeserializationStream {
  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) =
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}


private[hdm] class JavaSerializerInstance (
    counterReset: Int, extraDebugInfo: Boolean, defaultClassLoader: ClassLoader)
  extends SerializerInstance with Serializable {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * edited from org.apache.spark.serializer.JavaSerializer
 */
class JavaSerializer(conf: Config) extends Serializer with Externalizable {
  private var counterReset = 100
  private var extraDebugInfo = true

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(DynamicDependencyThreadFactory.defaultClassLoader())
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = try{
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = try {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}
