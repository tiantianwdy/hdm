package org.nicta.wdy.hdm.io

import java.io._
import java.nio.charset.Charset

import akka.serialization.Serializer
import org.codehaus.jackson.map.{SerializationConfig, ObjectMapper}
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.{JsonEncoding, JsonFactory}
import org.codehaus.jackson.JsonParser.Feature
import org.nicta.wdy.hdm.Arr
import sun.org.mozilla.javascript.json.JsonParser

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.reflect._

/**
 * Created by tiantian on 25/12/14.
 */
trait BlockSerializer[T] extends  Serializable {

  def identifier: Int

  def includeManifest: Boolean

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): T

  def toBinary(o: T): Array[Byte]

  def fromInputStream(in: InputStream): Seq[T]

  def toOutputStream(lst: Seq[T], out: OutputStream): Unit

  def iteratorInputStream(in: InputStream): Iterator[T]

}


class StringSerializer(val charsetName :String = "UTF-8") extends BlockSerializer[String] {
  
  override def identifier: Int = 9

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): String = {
    val str = new String(bytes, charsetName)
    str
  }

  override def toBinary(o: String): Array[Byte] = {
    o.getBytes(charsetName)
  }

  override def fromInputStream(in: InputStream): Seq[String] = {
    val reader = new BufferedReader(new InputStreamReader(in))
    var data = reader.readLine()
    var seq = ListBuffer.empty[String]
    while(data ne null) {
      seq += data
      data = reader.readLine()
    }
    seq
  }


  override def iteratorInputStream(in: InputStream): Iterator[String] = {
    new FileLineIterator(in)
  }

  override def toOutputStream(data:Seq[String],out: OutputStream): Unit = ???
}


class FileLineIterator(private val reader: BufferedReader) extends Iterator[String]{

  var nextLine:String = reader.readLine()

  def this(in: InputStream) {
    this(new BufferedReader(new InputStreamReader(in)))
  }

  override def hasNext: Boolean = {
    nextLine != null
  }

  override def next(): String = {
    val next = nextLine
    nextLine = reader.readLine
    next
  }
}

class JsonObjectSerializer[T:ClassTag] extends BlockSerializer[T] {

  val mapper = JacksonUtils.objectMapper

  val jsonFactory = JacksonUtils.jsonFactory



  override def identifier: Int = 10

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): T = {
    mapper.reader().readValue(bytes)
  }

  override def fromInputStream(in: InputStream): Seq[T] = {
    val objs = mapper.reader(classTag[T].runtimeClass).readValues(in)
    val buf = ArrayBuffer.empty[T]
    while(objs.hasNext){
      buf += objs.next()
    }
    buf
  }

  override def toBinary(o: T): Array[Byte] = {
    mapper.writer().writeValueAsBytes(o)
  }

  override def toOutputStream(lst: Seq[T], out: OutputStream): Unit = {
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

  override def iteratorInputStream(in: InputStream): Iterator[T] = {
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



object JacksonUtils {

  val jsonFactory = new JsonFactory()
  jsonFactory.configure(Feature.AUTO_CLOSE_SOURCE, false)
  lazy val objectMapper = new ObjectMapper(jsonFactory)


  def getJSONParser(is: InputStream) = {
    val jsonParser = jsonFactory.createJsonParser(is)
    jsonParser
  }


  def getJSONWriter(os:OutputStream) ={
    val jsonWriter = jsonFactory.createJsonGenerator(os, JsonEncoding.UTF8)
    jsonWriter
  }



}