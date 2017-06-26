package org.hdm.core.io.reader

import java.io.{InputStream, OutputStream}

import org.codehaus.jackson.JsonEncoding
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.util.DefaultPrettyPrinter

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

/**
  * Created by wu056 on 13/06/17.
  */
class JsonObjectReader extends BlockReader {

  val mapper = JacksonUtils.objectMapper

  val jsonFactory = JacksonUtils.jsonFactory



  override def identifier: Int = 10

  override def includeManifest: Boolean = false

  override def fromBinary[T:ClassTag](bytes: Array[Byte], manifest: Option[Class[_]]): T = {
    mapper.reader().readValue(bytes)
  }

  override def fromInputStream[T:ClassTag](in: InputStream): Seq[T] = {
    val objs = mapper.reader(classTag[T].runtimeClass).readValues(in)
    val buf = ArrayBuffer.empty[T]
    while(objs.hasNext){
      buf += objs.next()
    }
    buf
  }

  override def toBinary[T:ClassTag](o: T): Array[Byte] = {
    mapper.writer().writeValueAsBytes(o)
  }

  override def toOutputStream[T:ClassTag](lst: Seq[T], out: OutputStream): Unit = {
    val jsonGenerator = jsonFactory.createJsonGenerator(out, JsonEncoding.UTF8)
    mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false)
    jsonGenerator.setCodec(mapper)
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter)
    jsonGenerator.writeStartArray()
    lst.foreach{ elem =>
      jsonGenerator.writeObject(elem)
    }
    jsonGenerator.writeEndArray()
    jsonGenerator.flush()
    jsonGenerator.close()
  }

  override def iteratorInputStream[T:ClassTag](in: InputStream): Iterator[T] = {
    new JsonReaderIterator(in)
  }

  /**
   *
   * @param in
   * @tparam U
   */
  class JsonReaderIterator[U](in: InputStream) extends Iterator[U]{

    val objs = mapper.reader().readValues[U](in)

    override def hasNext: Boolean = {
      objs.hasNext
    }

    override def next(): U = {
     objs.next()
    }
  }

}
