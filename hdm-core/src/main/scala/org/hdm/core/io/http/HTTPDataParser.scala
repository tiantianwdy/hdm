package org.hdm.core.io.http

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.io.IOUtils
import org.hdm.core.Buf
import org.hdm.core.io.{BlockSerializer, DataParser, Path}
import org.hdm.core.storage.Block
import org.hdm.core.utils.Logging

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class HTTPDataParser extends DataParser with Logging {

  val httpClient = new HTTPConnector

  override def protocol: String = "http://"

  override def readBlock[T: ClassTag](path: Path, classLoader: ClassLoader)(implicit serializer: BlockSerializer[T]  = null): Block[T] = {
    require(path.protocol == "http://" || path.protocol == "https://")

    val res = httpClient.sendGet(path.toString,
      entity => {
        val is = entity.getContent
        serializer.fromInputStream(is)
      })
    Block(res)
  }

  override def readBlock[T: ClassTag, R: ClassTag](path: Path, func: (Iterator[T]) => Iterator[R], classLoader: ClassLoader)(implicit serializer: BlockSerializer[T] = null): Buf[R] = {

    require(path.protocol == "http://" || path.protocol == "https://")

    val res = httpClient.sendGet(path.toString,
      entity => {
        val is = entity.getContent
        serializer.fromInputStream(is)
      })
    func(res.toIterator).toBuffer
  }

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T]): Unit = {
    require(path.protocol == "http://" || path.protocol == "https://")
    val contents = bl.data.map(serializer.toBinary(_)).flatten.toArray
    val res  = httpClient.postBytes(path.toString, contents)
    log.info(s"Sending contents to $path finished with response code: $res")
  }
}
