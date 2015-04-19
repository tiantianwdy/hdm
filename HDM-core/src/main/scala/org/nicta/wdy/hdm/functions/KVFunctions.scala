package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.model.{PartialDep, FuncDependency, FullDep}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Buffer}
import scala.reflect.{classTag,ClassTag}

/**
 * Created by tiantian on 13/04/15.
 */
trait KVFunctions {

}


class ReduceByKey[T:ClassTag, K :ClassTag](fr: (T, T) => T) extends ParallelFunction[(K,T),(K,T)]{


  val dependency = FullDep

  override def apply(params: Seq[(K, T)]): Seq[(K, T)] = {
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
    tempMap.toSeq
  }

  override def aggregate(params: Seq[(K, T)], res: Buffer[(K, T)]): Buffer[(K, T)] = {
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

  override def aggregate(params: Seq[(K, T)], res: Buffer[(K, R)]): Buffer[(K, R)] = {
    res ++= apply(params)
  }

  override def apply(params: Seq[(K, T)]): Seq[(K, R)] = {
    params.map{ kv =>
      (kv._1, f(kv._2))
    }
  }
}

class MapKeys[T:ClassTag, K :ClassTag, R:ClassTag](f: K => R) extends ParallelFunction[(K,T),(R,T)] {
  
  override val dependency: FuncDependency = FullDep

  override def aggregate(params: Seq[(K, T)], res: mutable.Buffer[(R, T)]): mutable.Buffer[(R, T)] = ???

  override def apply(params: Seq[(K, T)]): Seq[(R, T)] = {
    params.map{ kv =>
      (f(kv._1), kv._2)
    }
  }
}

class FindByKey[T:ClassTag, K :ClassTag](val f: K => Boolean) extends ParallelFunction[(K,T),(K,T)] {

  val kType = classTag[K]

  val vType = classTag[T]
  
  override val dependency: FuncDependency = PartialDep 

  override def aggregate(params: Seq[(K, T)], res: mutable.Buffer[(K, T)]): mutable.Buffer[(K, T)] = {
    res ++= apply(params)
  }

  override def apply(params: Seq[(K, T)]): Seq[(K, T)] = {
    params.filter(kv => f(kv._1))
  }


}

class FindByValue[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,T),(K,T)] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Seq[(K, T)], res: mutable.Buffer[(K, T)]): mutable.Buffer[(K, T)] = {
    res ++= apply(params)
  }

  override def apply(params: Seq[(K, T)]): Seq[(K, T)] = {
    params.filter(kv => f(kv._2))
  }
}



class FindValuesByKey[T:ClassTag, K :ClassTag](f: T => Boolean) extends ParallelFunction[(K,Seq[T]),(K,Seq[T])] {
  
  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Seq[(K, Seq[T])], res: mutable.Buffer[(K, Seq[T])]): mutable.Buffer[(K, Seq[T])] = {
    res ++= apply(params)
  }

  override def apply(params: Seq[(K, Seq[T])]): Seq[(K, Seq[T])] = {
    params.map( kv => (kv._1, kv._2.filter(f)))
  }
}


