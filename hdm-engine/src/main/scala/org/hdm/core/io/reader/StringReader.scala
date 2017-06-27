package org.hdm.core.io.reader

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}

import scala.collection.mutable.ListBuffer


/**
  * Created by wu056 on 13/06/17.
  */
class StringReader(val charsetName :String = "UTF-8") extends BlockReader{
  import scala.reflect._

  override def identifier: Int = 9

  override def includeManifest: Boolean = false


  override def fromBinary[String](bytes: Array[Byte], manifest: Option[Class[_]])
                                 (implicit tag: ClassTag[String]):String = {
    val str = new java.lang.String(bytes, charsetName)
    str.asInstanceOf[String]
  }


  override def toBinary[String](o: String)
                               (implicit tag: ClassTag[String]): Array[Byte] = {
    o.toString.getBytes(charsetName)
  }

  override def fromInputStream[String](in: InputStream)
                                      (implicit tag: ClassTag[String]): Seq[String] = {
    val reader = new BufferedReader(new InputStreamReader(in))
    var data = reader.readLine()
    var seq = ListBuffer.empty[String]
    while(data ne null) {
      seq += data.asInstanceOf[String]
      data = reader.readLine()
    }
    seq
  }



  override def iteratorInputStream[String](in: InputStream)
                                          (implicit tag: ClassTag[String]): Iterator[String] = {
    new FileLineIterator(in).asInstanceOf[Iterator[String]]
  }

  override def toOutputStream[T](lst: Seq[T], out: OutputStream)(implicit tag: ClassTag[T]): Unit = ???

}


/**
  *
  * @param reader
  */
class FileLineIterator(private val reader: BufferedReader) extends Iterator[String] {

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
