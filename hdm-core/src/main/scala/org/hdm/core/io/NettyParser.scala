package org.hdm.core.io

import org.hdm.core.Buf
import org.hdm.core.storage.Block

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class NettyParser extends DataParser {

  override def protocol: String = Path.NETTY

  override def readBlock[T: ClassTag](path: Path,
                                      classLoader: ClassLoader)
                                     (implicit serializer: BlockSerializer[T] = null): Block[T] = {
    val iterator = new BufferedBlockIterator[T](blockRefs = Seq(path), classLoader = classLoader)
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
