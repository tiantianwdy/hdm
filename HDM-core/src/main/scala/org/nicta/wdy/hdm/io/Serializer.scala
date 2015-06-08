package org.nicta.wdy.hdm.io

import java.io._
import java.nio.charset.Charset

import akka.serialization.Serializer
import org.nicta.wdy.hdm.Buf

import scala.collection.mutable.ListBuffer

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

  override def toOutputStream(data:Seq[String],out: OutputStream): Unit = ???
}


