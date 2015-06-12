package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.collections.BufUtils
import org.nicta.wdy.hdm.model._


import scala.collection.mutable.HashMap
import scala.reflect.{classTag,ClassTag}

/**
 * Created by tiantian on 13/04/15.
 */
trait KVFunctions {

}


class ReduceByKey[T:ClassTag, K :ClassTag](fr: (T, T) => T) extends ParallelFunction[(K,T),(K,T)]{


  val dependency = FullDep

  override def apply(params: Buf[(K, T)]): Buf[(K, T)] = {
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
    tempMap.toBuffer
  }

  override def aggregate(params: Buf[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
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

  override def aggregate(params: Buf[(K, T)], res: Buf[(K, R)]): Buf[(K, R)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Buf[(K, T)]): Buf[(K, R)] = {
    params.map{ kv =>
      (kv._1, f(kv._2))
    }
  }
}

class MapKeys[T:ClassTag, K :ClassTag, R:ClassTag](f: K => R) extends ParallelFunction[(K,T),(R,T)] {
  
  override val dependency: FuncDependency = FullDep

  override def aggregate(params: Buf[(K, T)], res: Buf[(R, T)]): Buf[(R, T)] = ???

  override def apply(params: Buf[(K, T)]): Buf[(R, T)] = {
    params.map{ kv =>
      (f(kv._1), kv._2)
    }
  }
}

class FindByKey[T:ClassTag, K :ClassTag](val f: K => Boolean) extends ParallelFunction[(K,T),(K,T)] {

  val kType = classTag[K]

  val vType = classTag[T]
  
  override val dependency: FuncDependency = PartialDep 

  override def aggregate(params: Buf[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Buf[(K, T)]): Buf[(K, T)] = {
    params.filter(kv => f(kv._1))
  }


}

class FindByValue[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,T),(K,T)] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Buf[(K, T)], res: Buf[(K, T)]): Buf[(K, T)] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Buf[(K, T)]): Buf[(K, T)] = {
    params.filter(kv => f(kv._2))
  }
}



class FindValuesByKey[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,Buf[T]),(K,Buf[T])] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Buf[(K, Buf[T])], res: Buf[(K, Buf[T])]): Buf[(K, Buf[T])] = {
//    res ++= apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Buf[(K, Buf[T])]): Buf[(K, Buf[T])] = {
    params.map( kv => (kv._1, kv._2.filter(f)))
  }
}


