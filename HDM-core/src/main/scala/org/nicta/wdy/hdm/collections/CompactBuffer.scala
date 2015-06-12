package org.nicta.wdy.hdm.collections

import scala.reflect.ClassTag

/**
 * Created by tiantian on 11/06/15.
 */
class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable {

  // First two elements
  private var element0: T = _
  private var element1: T = _

  // Number of elements, including our two in the main object
  private var curSize = 0

  // Array for extra elements
  private var otherElements: Array[T] = null

  def bufferSize = if(otherElements == null) 0 else otherElements.length

  def apply(position: Int): T = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    if (position == 0) {
      element0
    } else if (position == 1) {
      element1
    } else {
      otherElements(position - 2)
    }
  }

  private def update(position: Int, value: T): Unit = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    if (position == 0) {
      element0 = value
    } else if (position == 1) {
      element1 = value
    } else {
      otherElements(position - 2) = value
    }
  }

  def += (value: T): CompactBuffer[T] = {
    val newIndex = curSize
    if (newIndex == 0) {
      element0 = value
      curSize = 1
    } else if (newIndex == 1) {
      element1 = value
      curSize = 2
    } else {
      growToSize(curSize + 1)
      otherElements(newIndex - 2) = value
    }
    this
  }

  def ++= (values: TraversableOnce[T]): CompactBuffer[T] = {
    values match {
      // Optimize merging of CompactBuffers, used in cogroup and groupByKey
      case compactBuf: CompactBuffer[T] =>
        val oldSize = curSize
        // Copy the other buffer's size and elements to local variables in case it is equal to us
        val itsSize = compactBuf.curSize
        val itsElements = compactBuf.otherElements
        growToSize(curSize + itsSize)
        if (itsSize == 1) {
          this(oldSize) = compactBuf.element0
        } else if (itsSize == 2) {
          this(oldSize) = compactBuf.element0
          this(oldSize + 1) = compactBuf.element1
        } else if (itsSize > 2) {
          this(oldSize) = compactBuf.element0
          this(oldSize + 1) = compactBuf.element1
          // At this point our size is also above 2, so just copy its array directly into ours.
          // Note that since we added two elements above, the index in this.otherElements that we
          // should copy to is oldSize.
          System.arraycopy(itsElements, 0, otherElements, oldSize, itsSize - 2)
        }

      case _ =>
        values.foreach(e => this += e)
    }
    this
  }

  override def length: Int = curSize

  override def size: Int = curSize

  override def iterator: Iterator[T] = new Iterator[T] {
    private var pos = 0
    override def hasNext: Boolean = pos < curSize
    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      pos += 1
      apply(pos - 1)
    }
  }

  /** Increase our size to newSize and grow the backing array if needed. */
  private def growToSize(newSize: Int): Unit = {
    if (newSize < 0) {
      throw new UnsupportedOperationException("Can't grow buffer past Int.MaxValue elements")
    }
    val capacity = if (otherElements != null) otherElements.length + 2 else 2
    if (newSize > capacity) {
      var newArrayLen = if(otherElements != null && otherElements.length > 0) otherElements.length * 1.1F else 8
      while (newSize - 2 > newArrayLen) {
        newArrayLen *= 1.1F
        if (newArrayLen == Int.MinValue) {
          // Prevent overflow if we double from 2^30 to 2^31, which will become Int.MinValue.
          // Note that we set the new array length to Int.MaxValue - 2 so that our capacity
          // calculation above still gives a positive integer.
          newArrayLen = Int.MaxValue - 2
        }
      }
      val newArray = new Array[T](newArrayLen.toInt)
      if (otherElements != null) {
        System.arraycopy(otherElements, 0, newArray, 0, otherElements.length)
      }
      otherElements = newArray
    }
    curSize = newSize
  }
}

object CompactBuffer {

  def apply[T: ClassTag](): CompactBuffer[T] = new CompactBuffer[T]

  def apply[T: ClassTag](value: T): CompactBuffer[T] = {
    val buf = new CompactBuffer[T]
    buf += value
  }

  def empty[T: ClassTag]() = new CompactBuffer[T]
}
