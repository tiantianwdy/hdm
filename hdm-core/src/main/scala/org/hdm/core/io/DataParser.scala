package org.hdm.core.io

import java.io.IOException
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.hdm.core.executor.{AppContext, HDMContext}
import org.hdm.core.functions.NullFunc
import org.hdm.core.io.hdfs.HDFSUtils
import org.hdm.core.io.http.HTTPDataParser
import org.hdm.core.io.reader.BlockReader
import org.hdm.core.model.{DDM, HDMInfo}
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.hdm.core.utils.Logging
import org.hdm.core.{Arr, Buf}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/1.
 */
trait DataParser {

  def protocol:String

  def readBlock[T: ClassTag](path:Path,
                             classLoader: ClassLoader)(implicit serializer:BlockReader):Block[T]

  def readBlock[T: ClassTag, R:ClassTag](path:Path,
                                         func:Iterator[T] => Iterator[R],
                                         classLoader: ClassLoader)(implicit serializer:BlockReader):Buf[R]

  def readBatch[T: ClassTag](path:Seq[Path])(implicit serializer:BlockReader):Seq[Block[T]] = ???

  def writeBlock[T: ClassTag](path:Path, bl:Block[T])(implicit serializer:BlockReader):Unit

}


object DataParser extends Logging{

  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)

  /**
    *
    * @param path
    * @param hDMContext
    * @param appContext
    * @return
    */
  def explainBlocks(path:Path, hDMContext: HDMContext, appContext:AppContext = AppContext()): Seq[DDM[String,String]] = {
    path.protocol match {
      case "hdm://" =>
        Seq(new DDM(id = path.name, location = path, func = new NullFunc[String], appContext = appContext, blocks = mutable.Buffer(hDMContext.localBlockPath + "/" + path.name)))
      case Path.HDFS =>
        HDFSUtils.getBlockLocations(path).map { p =>
          val id = hDMContext.newLocalId()
          new DDM(id,
            location = p.path,
            preferLocation = p.location,
            blockSize = p.size,
            func = new NullFunc[String],
            blocks = mutable.Buffer(hDMContext.localBlockPath + "/" + id),
            appContext = appContext)
        }
      case "file://" =>
        val id = path.name
        Seq(new DDM(id = id, location = path, func = new NullFunc[String], appContext = appContext, blocks = mutable.Buffer(hDMContext.localBlockPath + "/" + id)))
//      case "mysql://" =>
      case x => throw new IOException("Unsupported protocol:" + path.protocol)
    }
  }


  /**
    *
    * @param path
    * @param classLoader
    * @return
    */
  def readBlock(path:String, classLoader: ClassLoader):Block[_] = readBlock(Path(path), classLoader)


  /**
    *
    * @param path
    * @param classLoader
    * @return
    */
  def readBlock(path:Path, classLoader: ClassLoader):Block[_] = path.protocol.toLowerCase match {
    case Path.HDM => new HDMParser().readBlock(path, classLoader)
    case Path.HDFS => new HDFSParser().readBlock(path, classLoader)
    case Path.NETTY => new NettyParser().readBlock(path, classLoader)
    case Path.HTTP | Path.HTTPS => new HTTPDataParser().readBlock(path, classLoader)
    case _ => throw new IOException("Unsupported data protocol:" + path.protocol)
  }


  /**
    *
    * @param path
    * @param func
    * @param classLoader
    * @tparam R
    * @return
    */
  def readBlock[R:ClassTag](path:Path,
                            func:Arr[Any] => Arr[R],
                            classLoader: ClassLoader):Buf[R] = path.protocol.toLowerCase match {
    case Path.HDM => new HDMParser().readBlock(path, func, classLoader)
    case Path.HDFS => new HDFSParser().readBlock(path, func.asInstanceOf[Arr[String] => Arr[R]], classLoader)
    case Path.NETTY => new NettyParser().readBlock(path, func, classLoader)
    case Path.HTTP | Path.HTTPS => new HTTPDataParser().readBlock(path, func, classLoader)
    case _ => throw new IOException("Unsupported data protocol:" + path.protocol)
  }


  /**
    *
    * @param in
    * @param removeFromCache
    * @param classLoader
    * @return
    */
  def readBlock(in:HDMInfo,
                removeFromCache:Boolean,
                classLoader: ClassLoader):Block[_] = {
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
          val bl = DataParser.readBlock(in.location, classLoader: ClassLoader)
          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl

        case Path.NETTY=>
          log.info(s"reading Netty block from ${in.location} with class loader ${classLoader}")
          val bl = DataParser.readBlock(in.location, classLoader)
          log.info(s"finished reading block with size: ${Block.byteSize(bl)/ (1024*1024F)} MB. ")
          bl

        case Path.HTTP | Path.HTTPS =>
          log.info(s"reading http block from ${in.location} with class loader ${classLoader}")
          val bl = DataParser.readBlock(in.location, classLoader)
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


  /**
    *
    * @param in
    * @param removeFromCache
    * @param func
    * @param classLoader
    * @tparam R
    * @return
    */
  def readBlock[R:ClassTag](in:HDMInfo,
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

        case Path.HTTP | Path.HTTPS =>
          DataParser.readBlock(in.location, func, classLoader)
      }
    } else {
      log.info(s"input data are at local: [${in.id}] ")
      val resp = HDMBlockManager().getBlock(in.id)
      if(removeFromCache) HDMBlockManager().removeBlock(in.id)
      func(resp.asInstanceOf[Block[Any]].data.toIterator).toBuffer
    }
  }


  /**
    *
    * @param path
    * @param bl
    */
  def writeBlock(path:String, bl:Block[_]):Unit = writeBlock(Path(path), bl)


  /**
    *
    * @param path
    * @param bl
    */
  def writeBlock(path:Path, bl:Block[_]):Unit = path.protocol match {
    case "hdm://" =>
    case "hdfs://" =>
    case "file://" =>
    case "mysql://" =>
    case _ =>
  }

}