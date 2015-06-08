package org.nicta.wdy

/**
 * Created by tiantian on 5/06/15.
 */
package object hdm {

/*  type Buf[A] = scala.Array[A]
  val Buf = scala.Array*/
  type Buf[A] = scala.collection.mutable.Buffer[A]
  val Buf = scala.collection.mutable.Buffer
}
