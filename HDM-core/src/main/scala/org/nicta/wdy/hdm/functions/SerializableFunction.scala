package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.Buf

import scala.collection.mutable.Buffer

/**
 * Created by tiantian on 2/03/15.
 */

/**
 * basic interface for generic functions with a serializable feature
 * @tparam R return type
 */
trait SerializableFunction[I,R] extends  Serializable{

  def apply(params: I) : R

}


/**
 * interface for a function to support calculating the results in an aggregation manner
 * @tparam T
 * @tparam R
 */
trait Aggregatable[T,R] extends  Serializable {


  def aggregate(params:T, res:R): R


}

/**
 * an advanced interface for a function to support calculating the results in an aggregation manner
 * support to do some pre and post processing before and after data are collected by the [[Aggregator]]
 * @tparam T
 * @tparam R
 */
trait Aggregator[T, R] extends  Serializable {

  /**
   * initiate the aggregator with starting data
   * @param zero
   */
  def init(zero:R)

  /**
   * performing aggregation with new input data
   * @param params
   */
  def aggregate(params:T): Unit

  /**
   * get the final result from this aggregator, this may trigger some delayed aggregation processes due to performance consideration in previous aggregation steps
   * @return
   */
  def result: R

}


/**
 * an interface for functions to support merging result from the results of multiple sub-functions
 * @tparam R
 */
trait Mergeable[R]{

  def merge(params:Seq[R], res:Buffer[R]): Buffer[R]

}


/**
 * factory object for [[SerializableFunction]], place holder for adding new constructors when needed
 */
object SerializableFunction extends  Serializable{



}