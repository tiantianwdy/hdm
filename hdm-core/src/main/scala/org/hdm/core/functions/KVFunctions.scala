package org.hdm.core.functions

import org.hdm.core.{Buf, Arr}
import org.hdm.core.collections.BufUtils
import org.hdm.core.model._


import scala.collection.mutable.HashMap
import scala.reflect.{classTag,ClassTag}

/**
 * Created by tiantian on 13/04/15.
 */
trait KVFunctions {

}


class ReduceByKey[T:ClassTag, K :ClassTag](fr: (T, T) => T) extends ParallelFunction[(K,T),(K,T)]{


  val dependency = FullDep

  override def apply(params: Arr[(K, T)]): Arr[(K, T)] = {
    val tempMap = HashMap.empty[K,T]
    params foreach{ elem =>
      val k = elem._1
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
        tempMap.update(k, fr(v, elem._2))
      } else {
        tempMap += elem
      }
    }
    tempMap.toIterator
  }

  override def aggregate(params: Arr[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
    val tempMap = HashMap.empty[K,T] ++= res
    params foreach { elem =>
      val k = elem._1
      if (tempMap.contains(k)) {
        val v = tempMap.apply(k)
        tempMap.update(k, fr(v, elem._2))
      } else {
        tempMap += elem
      }
    }
    tempMap.toBuffer
  }
}

class MapValues[T:ClassTag, K :ClassTag, R:ClassTag](f: T => R) extends ParallelFunction[(K,T),(K,R)] {
  
  override val dependency: FuncDependency = FullDep

  override def aggregate(params: Arr[(K, T)], res: Buf[(K, R)]): Buf[(K, R)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Arr[(K, T)]): Arr[(K, R)] = {
    params.map{ kv =>
      (kv._1, f(kv._2))
    }
  }
}

class MapKeys[T:ClassTag, K :ClassTag, R:ClassTag](f: K => R) extends ParallelFunction[(K,T),(R,T)] {
  
  override val dependency: FuncDependency = FullDep

  override def aggregate(params: Arr[(K, T)], res: Buf[(R, T)]): Buf[(R, T)] = ???

  override def apply(params: Arr[(K, T)]): Arr[(R, T)] = {
    params.map{ kv =>
      (f(kv._1), kv._2)
    }
  }
}

class FindByKey[T:ClassTag, K :ClassTag](val f: K => Boolean) extends ParallelFunction[(K,T),(K,T)] {

  val kType = classTag[K]

  val vType = classTag[T]
  
  override val dependency: FuncDependency = PartialDep 

  override def aggregate(params: Arr[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Arr[(K, T)]): Arr[(K, T)] = {
    params.filter(kv => f(kv._1))
  }


}

class FindByValue[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,T),(K,T)] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Arr[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Arr[(K, T)]): Arr[(K, T)] = {
    params.filter(kv => f(kv._2))
  }
}



class FindValuesByKey[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,Iterable[T]),(K,Iterable[T])] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Arr[(K, Iterable[T])], res: Buf[(K, Iterable[T])]): Buf[(K, Iterable[T])] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Arr[(K, Iterable[T])]): Arr[(K, Iterable[T])] = {
    params.map( kv => (kv._1, kv._2.filter(f)))
  }
}


