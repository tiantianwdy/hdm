package org.nicta.wdy.hdm

import org.nicta.wdy.hdm.utils.ClosureCleaner


/**
 * Created by Tiantian on 2014/5/25.
 */
class ComplexHDM[AnyVal](val elems: List[HDM[AnyVal]]) extends HDM[AnyVal] {



  def this(elem: Array[HDM[AnyVal]]) {
    this(elem.toList)
  }

  override def children = elems

  override def apply[AnyVal, U](f: (AnyVal) => U): HDM[U] = {
    HDM(elems.map {
      hdm => hdm.location match {
        case Local => hdm.apply(f)
        case Remote =>
          ClosureCleaner.apply(f)
          // send f to remote actor
          val path = "remotePath"
          HDM(path)
      }
    }.toArray)
  }

  override def shuffle(partitioner: Partitioner): HDM[AnyVal] = ???

  override def collect(): HDM[AnyVal] = ???

  override def flatMap[U](f: (AnyVal) => U): HDM[U] = ???

  override def isLeaf: Boolean = ???

  override def location: Location = ???

  override def distribution: Distribution = ???
}

