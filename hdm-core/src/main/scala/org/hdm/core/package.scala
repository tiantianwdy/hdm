package org.hdm

/**
 * Created by tiantian on 5/06/15.
 */
package object core {

/*  type Buf[A] = scala.Array[A]
  val Buf = scala.Array*/
  type Arr[A] = scala.collection.Iterator[A]
  val Arr = org.hdm.core.collections.Iterator

  type Buf[A] = scala.collection.mutable.Buffer[A]
  val Buf = scala.collection.mutable.Buffer

  type Blk[A] = scala.collection.mutable.Buffer[A]
  val Blk = scala.collection.mutable.Buffer
}
