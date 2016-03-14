package org.nicta.wdy.hdm.io

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.nicta.wdy.hdm.{Buf, Arr}
import org.nicta.wdy.hdm.functions.NullFunc
import org.nicta.wdy.hdm.io.hdfs.HDFSUtils
import org.nicta.wdy.hdm.model.{HDM, DDM}
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Block}
import akka.serialization.Serializer
import java.io.IOException

import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/1.
 */
trait DataParser {

  def protocol:String

  def readBlock[T: ClassTag](path:Path)(implicit serializer:BlockSerializer[T]):Block[T]

  def readBlock[T: ClassTag, R:ClassTag](path:Path,
                                         func:Iterator[T] => Iterator[R],
                                         classLoader: ClassLoader)(implicit serializer:BlockSerializer[T]):Buf[R]

  def readBatch[T: ClassTag](path:Seq[Path])(implicit serializer:BlockSerializer[T]):Seq[Block[T]] = ???

  def writeBlock[T: ClassTag](path:Path, bl:Block[T])(implicit serializer:BlockSerializer[T]):Unit

}

class HdfsParser extends DataParser{

  type HPath = org.apache.hadoop.fs.Path

  override def writeBlock[String: ClassTag](path: Path, bl: Block[String])(implicit serializer: BlockSerializer[String]): Unit = ???

  override def readBlock[String: ClassTag](path: Path)(implicit serializer: BlockSerializer[String] = new StringSerializer): Block[String] = {
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
                                                         classLoader: ClassLoader)(implicit serializer: BlockSerializer[String] = new StringSerializer): Buf[R] = {
    val conf = new Configuration()
    conf.set("fs.default.name", path.protocol + path.address)
    val filePath = new HPath(path.relativePath)
    val fs = FileSystem.get(conf)
    //    val status = fs.getFileStatus(filePath)
    //    val buffer = ByteBuffer.allocate(status.getBlockSize.toInt)
    //    fileInputStream.read(buffer)
    //    val data = serializer.fromBinary(buffer.array())
    val fileInputStream = fs.open(filePath)
    val data = func(serializer.iteratorInputStream(fileInputStream))
    data.toBuffer
  }


  override def readBatch[String: ClassTag](pathList: Seq[Path])(implicit serializer: BlockSerializer[String] = new StringSerializer): Seq[Block[String]] = {
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

  override def protocol: String = "hdfs://"

}

class FileParser extends DataParser{

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T]): Unit = ???


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockSerializer[T]): Buf[R] = ???

  override def readBlock[T: ClassTag](path: Path)(implicit serializer: BlockSerializer[T]): Block[T] = ???

  override def protocol: String = "file://"

}

class MysqlParser extends DataParser{

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T]): Unit = ???


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockSerializer[T]): Buf[R] = ???

  override def readBlock[T: ClassTag](path: Path)(implicit serializer: BlockSerializer[T]): Block[T] = ???

  override def protocol: String = "mysql://"
}


class HDMParser extends DataParser {

  lazy val blockManager = HDMBlockManager()

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T] = null): Unit = {
    blockManager.add(bl.id, bl)
  }


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockSerializer[T] = null): Buf[R] = ???

  override def readBlock[T: ClassTag](path: Path)(implicit serializer: BlockSerializer[T] = null): Block[T] = {
    blockManager.getBlock(path.name).asInstanceOf[Block[T]]
  }

  override def protocol: String = "hdm://"
}


class NettyParser extends DataParser {

  override def protocol: String = "netty://"

  override def readBlock[T: ClassTag](path: Path)(implicit serializer: BlockSerializer[T] = null): Block[T] = {
    val iterator = new BufferedBlockIterator[T](Seq(path))
    val data = iterator.toSeq
    val id = path.name
    Block(id, data)
  }


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockSerializer[T] = null): Buf[R] = {
    val iterator = new BufferedBlockIterator[T](blockRefs = Seq(path), classLoader = classLoader)
    val data = func(iterator)
    val id = path.name
    data.toBuffer
  }

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T]): Unit = ???
}

object DataParser extends Logging{

  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)

  def explainBlocks(path:Path): Seq[DDM[String,String]] = {
    path.protocol match {
      case "hdm://" =>
        Seq(new DDM(id = path.name, location = path, func = new NullFunc[String]))
      case "hdfs://" =>
        HDFSUtils.getBlockLocations(path).map(p => new DDM(location = p.path, preferLocation = p.location, blockSize = p.size, func = new NullFunc[String]))
      case "file://" =>
        Seq(new DDM(id = path.name, location = path, func = new NullFunc[String]))
//      case "mysql://" =>
      case x => throw new IOException("Unsupported protocol:" + path.protocol)
    }
  }

  def readBlock(path:String):Block[_] = readBlock(Path(path))

  def readBlock(path:Path):Block[_] = path.protocol.toLowerCase match {
    case "hdm://" => new HDMParser().readBlock(path)
    case "hdfs://" => new HdfsParser().readBlock(path)
    case "netty://" => new NettyParser().readBlock(path)
//    case "file://" => new FileParser().readBlock(path)
//    case "mysql://" => new MysqlParser().readBlock(path)
    case _ => throw new IOException("Unsupported data protocol:" + path.protocol)
  }

  def readBlock[R:ClassTag](path:Path,
                            func:Arr[Any] => Arr[R],
                            classLoader: ClassLoader):Buf[R] = path.protocol.toLowerCase match {
    case "hdm://" => new HDMParser().readBlock(path, func, classLoader)
    case "hdfs://" => new HdfsParser().readBlock(path, func.asInstanceOf[Arr[String] => Arr[R]], classLoader)
    case "netty://" => new NettyParser().readBlock(path, func, classLoader)
    //    case "file://" => new FileParser().readBlock(path)
    //    case "mysql://" => new MysqlParser().readBlock(path)
    case _ => throw new IOException("Unsupported data protocol:" + path.protocol)
  }
  
  def readBlock(in:HDM[_,_], removeFromCache:Boolean):Block[_] = {
    if (!HDMBlockManager().isCached(in.id)) {
      in.location.protocol match {
        case Path.AKKA =>
          //todo replace with using data parsers
          log.info(s"Asking block ${in.location.name} from ${in.location.parent}")
          val await = HDMIOManager().askBlock(in.location.name, in.location.parent) // this is only for hdm
          Await.result[Block[_]](await, maxWaitResponseTime) match {
            case data: Block[_] => data
//              val resp = HDMBlockManager().getBlock(id)
//              if(removeFromCache) HDMBlockManager().removeBlock(id)
//              resp
            case _ => throw new RuntimeException(s"Failed to get data from ${in.location.name}")
          }

        case Path.HDFS =>
          val bl = DataParser.readBlock(in.location)
          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl

        case Path.NETTY=>
          val bl = DataParser.readBlock(in.location)
          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl
      }
    } else {
      log.info(s"input data are at local: [${in.id}] ")
      val resp = HDMBlockManager().getBlock(in.id)
      if(removeFromCache) HDMBlockManager().removeBlock(in.id)
      resp
    }
  }

  def readBlock[R:ClassTag](in:HDM[_,_],
                            removeFromCache:Boolean,
                            func:Arr[Any] => Arr[R],
                            classLoader: ClassLoader):Buf[R] = {
    if (!HDMBlockManager().isCached(in.id)) {
      in.location.protocol match {
        case Path.AKKA =>
          //todo replace with using data parsers
          log.info(s"Asking block ${in.location.name} from ${in.location.parent}")
          val await = HDMIOManager().askBlock(in.location.name, in.location.parent) // this is only for hdm
          Await.result[Block[_]](await, maxWaitResponseTime) match {
            case data: Block[_] =>
//              Block(data.id, func(data.asInstanceOf[Block[Any]].data.toIterator))
              func(data.asInstanceOf[Block[Any]].data.toIterator).toBuffer
            case _ => throw new RuntimeException(s"Failed to get data from ${in.location.name}")
          }

        case Path.HDFS =>
          val bl = DataParser.readBlock(in.location, func, classLoader)
//          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl.toBuffer

        case Path.NETTY=>
          val bl = DataParser.readBlock(in.location, func, classLoader)
//          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl.toBuffer
      }
    } else {
      log.info(s"input data are at local: [${in.id}] ")
      val resp = HDMBlockManager().getBlock(in.id)
      if(removeFromCache) HDMBlockManager().removeBlock(in.id)
      func(resp.asInstanceOf[Block[Any]].data.toIterator).toBuffer
    }
  }


  def writeBlock(path:String, bl:Block[_]):Unit = writeBlock(Path(path), bl)

  def writeBlock(path:Path, bl:Block[_]):Unit = path.protocol match {
    case "hdm://" =>
    case "hdfs://" =>
    case "file://" =>
    case "mysql://" =>
    case _ =>
  }

}