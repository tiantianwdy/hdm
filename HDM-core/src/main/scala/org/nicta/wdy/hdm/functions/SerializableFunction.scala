package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.Buf

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

}

trait Aggregatable[T,R] extends  Serializable {


  def aggregate(params:T, res:R): R


}

trait Aggregator[T, R] extends  Serializable {

  def init(zero:R)

  def aggregate(params:T): Unit

  def result: R

}

trait Mergeable[R]{

  def merge(params:Seq[R], res:Buffer[R]): Buffer[R]

}
