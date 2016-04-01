package org.nicta.wdy.hdm.collections

import org.nicta.wdy.hdm.Arr

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by tiantian on 5/06/15.
 */
object BufUtils {

  def combine[T: ClassTag](c1:Array[T], c2:Array[T]):Array[T] = {
    val combinedArr = new Array[T](c1.length + c2.length)
    System.arraycopy(c1, 0, combinedArr, 0, c1.length)
    System.arraycopy(c2, 0, combinedArr, c1.length, c2.length)
    combinedArr
  }

  def add[T: ClassTag](c1:Array[T], elem:T):Array[T] = {
    val combinedArr = new Array[T](c1.length + 1)
    System.arraycopy(c1, 0, combinedArr, 0, c1.length)
    combinedArr.update(c1.length, elem)
    combinedArr
  }

  def combine[T: ClassTag](c1:mutable.Buffer[T], c2:mutable.Buffer[T]):mutable.Buffer[T] = {
//    if(c1.length >= c2.length)
      c1 ++= c2
//    else c2 ++= c1
  }

  def  add[T: ClassTag](c1:mutable.Buffer[T], elem:T):mutable.Buffer[T] = {
    c1 += elem
  }

  def combine[T: ClassTag](c1:mutable.Buffer[T], c2:Iterator[T]):mutable.Buffer[T] = {
    //    if(c1.length >= c2.length)
    c1 ++= c2
    //    else c2 ++= c1
  }

  def combine[T: ClassTag](c1:Seq[T], c2:Seq[T]):Seq[T] = {
    val combinedArr = new Array[T](c1.length + c2.length)
    c1.copyToArray(combinedArr, 0, c1.length)
    c2.copyToArray(combinedArr, c1.length, c2.length)
    combinedArr
  }
  
  def add[T: ClassTag](c1:Seq[T], elem:T):Seq[T] = {
    val combinedArr = new Array[T](c1.length + 1)
    c1.copyToArray(combinedArr, 0, c1.length)
    combinedArr.update(c1.length, elem)
    combinedArr
  }

  def toBuf[K,V](map:mutable.Map[K,V]):Arr[(K,V)] = {
    if(classOf[Arr[(K,V)]] == classOf[Array[(K,V)]]){
      map.toArray[(K,V)].asInstanceOf[Arr[(K,V)]]
    } else {
      map.toBuffer[(K,V)].asInstanceOf[Arr[(K,V)]]
    }

  }


}

object Iterator {


  def apply[A:ClassTag](elem:A):Iterator[A] = {
    Seq(elem).toIterator
  }


  def apply[A:ClassTag](elems:Traversable[A]):Iterator[A]  = {
    elems.toIterator
  }

  def empty[A:ClassTag]:Iterator[A] = Iterator.empty[A]
}
