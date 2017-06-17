package org.hdm.core.io

import org.hdm.core.Buf
import org.hdm.core.io.reader.BlockReader
import org.hdm.core.storage.Block

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class FileParser extends DataParser{

  override def writeBlock[T: ClassTag](path: Path, bl: Block[T])(implicit serializer: BlockReader): Unit = ???


  override def readBlock[T: ClassTag, R: ClassTag](path: Path,
                                                   func: (Iterator[T]) => Iterator[R],
                                                   classLoader: ClassLoader)(implicit serializer: BlockReader): Buf[R] = ???

  override def readBlock[T: ClassTag](path: Path, classLoader: ClassLoader)(implicit serializer: BlockReader): Block[T] = ???

  override def protocol: String = "file://"

}
