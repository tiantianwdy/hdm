package org.hdm.core.io

import org.hdm.core.Buf
import org.hdm.core.storage.{Block, HDMBlockManager}

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class HDMParser extends DataParser {

  lazy val blockManager = HDMBlockManager()

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockSerializer[T] = null): Unit = {
    blockManager.add(bl.id, bl)
  }


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockSerializer[T] = null): Buf[R] = ???

  override def readBlock[T: ClassTag](path: Path, classLoader: ClassLoader)(implicit serializer: BlockSerializer[T] = null): Block[T] = {
    blockManager.getBlock(path.name).asInstanceOf[Block[T]]
  }

  override def protocol: String = "hdm://"
}
