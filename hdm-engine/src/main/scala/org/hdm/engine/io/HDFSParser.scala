package org.hdm.core.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.hdm.core.Buf
import org.hdm.core.io.reader.{BlockReader, StringReader}
import org.hdm.core.storage.Block

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class HDFSParser extends DataParser {

  type HPath = org.apache.hadoop.fs.Path

  override def protocol: String = Path.HDFS

  override def writeBlock[String: ClassTag](path: Path, bl: Block[String])(implicit serializer: BlockReader): Unit = ???

  override def readBlock[String: ClassTag](path: Path,  classLoader: ClassLoader)(implicit serializer: BlockReader = new StringReader): Block[String] = {
    val conf = new Configuration()
    conf.set("fs.default.name", path.protocol + path.address)
    val filePath = new HPath(path.relativePath)
    val fs = FileSystem.get(conf)
    //    val status = fs.getFileStatus(filePath)
    //    val buffer = ByteBuffer.allocate(status.getBlockSize.toInt)
    //    fileInputStream.read(buffer)
    //    val data = serializer.fromBinary(buffer.array())
    val fileInputStream = fs.open(filePath)
    val data = serializer.fromInputStream(fileInputStream)
    Block(data)
  }


  override  def readBlock[String: ClassTag, R: ClassTag](path: Path,
                                                         func:Iterator[String] => Iterator[R],
                                                         classLoader: ClassLoader)(implicit serializer: BlockReader = new StringReader): Buf[R] = {
    val conf = new Configuration()
    conf.set("fs.default.name", path.protocol + path.address)
    val filePath = new HPath(path.relativePath)
    val fs = FileSystem.get(conf)
    //    val status = fs.getFileStatus(filePath)
    //    val buffer = ByteBuffer.allocate(status.getBlockSize.toInt)
    //    fileInputStream.read(buffer)
    //    val data = serializer.fromBinary(buffer.array())
    val fileInputStream = fs.open(filePath)
    val data = func(serializer.iteratorInputStream[String](fileInputStream))
    data.toBuffer
  }


  override def readBatch[String: ClassTag](pathList: Seq[Path])(implicit serializer: BlockReader = new StringReader): Seq[Block[String]] = {
    val head = pathList.head
    val conf = new Configuration()
    conf.set("fs.default.name", head.protocol + head.address)
    val fs = FileSystem.get(conf)
    pathList.map{path =>
      val filePath = new HPath(path.relativePath)
      val fileInputStream = fs.open(filePath)
      val data = serializer.fromInputStream(fileInputStream)
      Block(data)
    }
  }


}
