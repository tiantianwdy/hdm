package org.nicta.wdy.hdm.functions

import scala.collection.mutable.Buffer

/**
 * Created by tiantian on 2/03/15.
 */

/**
 *
 * @tparam R return type
 */
trait SerializableFunction[I,R] extends  Serializable{

  def apply(params: I) : R

  //  def andThen[U](func: SerializableFunction[R,U]): SerializableFunction[I, U] = ???
}

trait Aggregatable[T,R] {

  def aggregate(params:Seq[T], res:Buffer[R]): Buffer[R]

}

trait Mergeable[R] {

  def merge(params:Seq[R], res:Buffer[R]): Buffer[R]
}
