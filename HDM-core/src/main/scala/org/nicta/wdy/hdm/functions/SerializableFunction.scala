package org.nicta.wdy.hdm.functions

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
