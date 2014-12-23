package org.nicta.wdy.hdm.io

import org.nicta.wdy.hdm.storage.Block
import akka.serialization.Serializer
import java.io.IOException

/**
 * Created by Tiantian on 2014/12/1.
 */
trait DataParser {

  def protocol:String

  def readBlock[T](path:String)(implicit serializer:Serializer):Block[T]

  def writeBlock[T](path:String, bl:Block[T])(implicit serializer:Serializer):Unit

}

class HdfsParser extends DataParser{

  override def writeBlock[T](path: String, bl: Block[T])(implicit serializer: Serializer): Unit = ???

  override def readBlock[T](path: String)(implicit serializer: Serializer): Block[T] = ???

  override def protocol: String = "hdfs://"

}

class FileParser extends DataParser{

  override def writeBlock[T](path: String, bl: Block[T])(implicit serializer: Serializer): Unit = ???

  override def readBlock[T](path: String)(implicit serializer: Serializer): Block[T] = ???

  override def protocol: String = "file://"

}

class MysqlParser extends DataParser{

  override def writeBlock[T](path: String, bl: Block[T])(implicit serializer: Serializer): Unit = ???

  override def readBlock[T](path: String)(implicit serializer: Serializer): Block[T] = ???

  override def protocol: String = "mysql://"
}


class HDMParser extends DataParser {

  override def writeBlock[T](path: String, bl: Block[T])(implicit serializer: Serializer): Unit = ???

  override def readBlock[T](path: String)(implicit serializer: Serializer): Block[T] = ???

  override def protocol: String = "hdm://"
}


object DataParser{

  def readBlock[T](path:String)(implicit serializer:Serializer):Block[T] = readBlock(Path(path))

  def readBlock[T](path:Path)(implicit serializer:Serializer):Block[T] = path.protocol match {
    case "hdm://" => new HDMParser().readBlock(Path.toString)
    case "hdfs://" => new HdfsParser().readBlock(Path.toString)
    case "file://" => new FileParser().readBlock(Path.toString)
    case "mysql://" => new MysqlParser().readBlock(Path.toString)
    case _ => throw new IOException("Unsupported data protocol:" + path.protocol)
  }

  def writeBlock[T](path:String, bl:Block[T])(implicit serializer:Serializer):Unit = writeBlock(Path(path), bl)

  def writeBlock[T](path:Path, bl:Block[T])(implicit serializer:Serializer):Unit = path.protocol match {
    case "hdm://" =>
    case "hdfs://" =>
    case "file://" =>
    case "mysql://" =>
    case _ =>
  }

}