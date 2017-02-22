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

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, ClosureSerializer}
import com.esotericsoftware.minlog.Log

import com.twitter.chill.{ScalaKryoInstantiator, AllScalaRegistrar, EmptyScalaKryoInstantiator}
import com.typesafe.config.Config
import org.hdm.core.model.{DDM, DFM, HDM}
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * edited from org.apache.spark.serializer.KryoSerializer
 */
class KryoSerializer(conf: Config)
  extends org.hdm.core.serializer.Serializer
  with Serializable {

  private val bufferSize = (0.064 * 1024 * 1024).toInt

  private val maxBufferSize =  64 * 1024 * 1024 //MB
  private val referenceTracking = true
  private val registrationRequired = false
//  private val userRegistrator = conf.getOption("spark.kryo.registrator")
//  private val classesToRegister = conf.get("spark.kryo.classesToRegister", "")
//    .split(',')
//    .filter(!_.isEmpty)

  def newKryoOutput() = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))

  def newKryo(): Kryo = {
//    val instantiator = new EmptyScalaKryoInstantiator
    val instantiator = new ScalaKryoInstantiator()
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy())

    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }

    for (tup <- KryoSerializer.serializerMap) {
      kryo.register(tup._1, tup._2)
    }
    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)
    kryo.register(classOf[ClosureSerializer.Closure], new ClosureSerializer())

//    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())

    try {
      // Use the default classloader when calling the user registrator.
      Thread.currentThread.setContextClassLoader(classLoader)
      // Register classes given through spark.kryo.classesToRegister.
//      classesToRegister
//        .foreach { className => kryo.register(Class.forName(className, true, classLoader)) }
      // Allow the user to register their own classes by setting spark.kryo.registrator.
//      userRegistrator
//        .map(Class.forName(_, true, classLoader).newInstance().asInstanceOf[KryoRegistrator])
//        .foreach { reg => reg.registerClasses(kryo) }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to register classes with Kryo", e)
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }

    // Register Chill's classes; we do this after our ranges and the user's own classes to let
    // our code override the generic serializers in Chill for things like Seq
    new AllScalaRegistrar().apply(kryo)

    kryo.setClassLoader(classLoader)
//    Log.TRACE()
    kryo
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }
}

private[hdm]
class KryoSerializationStream(kryo: Kryo, outStream: OutputStream) extends SerializationStream {
  val output = new KryoOutput(outStream)

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() { output.flush() }
  override def close() { output.close() }
}

private[hdm]
class KryoDeserializationStream(kryo: Kryo, inStream: InputStream) extends DeserializationStream {
  private val input = new KryoInput(inStream)

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    // Kryo's Input automatically closes the input stream it is using.
    input.close()
  }
}

private[hdm] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {

  private val _kryo = new ThreadLocal[Kryo]()

  _kryo.set(ks.newKryo())
//  private val kryo = ks.newKryo()
  def kryo = _kryo.get()

  // Make these lazy vals to avoid creating a buffer unless we use them
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    input.setBuffer(bytes.array)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val oldClassLoader = kryo.getClassLoader
    kryo.setClassLoader(loader)
    input.setBuffer(bytes.array)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    kryo.setClassLoader(oldClassLoader)
    obj
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, s)
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

private[serializer] object KryoSerializer {
  import scala.collection.JavaConversions._
  // Commonly used classes.
  private val toRegister: mutable.Buffer[Class[_]] = new CopyOnWriteArrayList[Class[_]]()
  
  private val serializerMap: mutable.Map[Class[_], com.esotericsoftware.kryo.Serializer[_]] = new ConcurrentHashMap[Class[_], com.esotericsoftware.kryo.Serializer[_]]()

  toRegister ++=  mutable.Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Long]],
    classOf[HDM[_]],
    classOf[DFM[_,_]],
    classOf[DDM[_, _]],
    classOf[java.lang.invoke.SerializedLambda]
  )
  
  def registerClass(cls: Class[_]): Unit = {
    toRegister += cls
  }
  
  
  def registerSerializer(cls: Class[_], serializer: com.esotericsoftware.kryo.Serializer[_]): Unit = {
    serializerMap += cls -> serializer
  }
  
}

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper.
 * Kryo deserializes this into an AbstractCollection, which unfortunately doesn't work.
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import org.hdm.core.serializer.JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
    : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] =>
        scala.collection.JavaConversions.asJavaIterable(scalaIterable)
      case javaIterable: java.lang.Iterable[_] =>
        javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer {
  // The class returned by asJavaIterable (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass =
    scala.collection.convert.WrapAsJava.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        System.err.println("Failed to find the underlying field in " + wrapperClass, e)
        None
    }
  }
}
