package org.nicta.wdy.hdm.io

import org.nicta.wdy.hdm.storage.BlockRef

/**
 * Created by Tiantian on 2014/12/9.
 *
 * This iterator is used to read a set of distributed DDMs but acts as a local iterator
 *
 * implemented as a cycle buffer with size factor
 */
class BufferedBlockIterator[+A](val factor:Int = 3, protected val blockRefs: Seq[BlockRef]) extends BufferedIterator[A]{

  def init() = ??? // read the first buffer segment

  def load(block:BlockRef) = ??? // load a block when necessary

  override def head: A = ???

  override def next(): A = ???

  override def hasNext: Boolean = ???
}
