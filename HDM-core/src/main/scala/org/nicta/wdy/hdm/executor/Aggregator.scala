package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.functions.{ShuffleAggregatable, Aggregatable}

import scala.collection.mutable

/**
 * Created by tiantian on 27/04/15.
 */
trait Aggregator[T,R] {


  def aggregate(iterator: Iterator[T]):Unit

  def getResult():R

}

class ShuffleBlockAggregator[K,T,V] (val mutableMap: mutable.HashMap[K,V] = mutable.HashMap.empty[K,V],
                                     val f: T=>K,
                                     val zero:T=>V,
                                     val aggr: (T,V) => V) extends Aggregator[Seq[T],Seq[(K,V)]] {

  def this(func: ShuffleAggregatable[K,T,V]){
    this(mutableMap = new mutable.HashMap[K,V], func.map(_), func.zero(_), func.aggregate(_,_) )
  }

  override def aggregate(iterator: Iterator[Seq[T]]): Unit = {
    while(iterator.hasNext){
      iterator.next() foreach{ elem =>
        val k = f(elem)
        if(mutableMap.contains(k)){
          val v = mutableMap.apply(k)
          mutableMap.update(k, aggr(elem,v))
        } else {
          mutableMap += k -> zero(elem)
        }
      }
    }
  }

  override def getResult(): Seq[(K,V)] = {
    mutableMap.toSeq
  }
}