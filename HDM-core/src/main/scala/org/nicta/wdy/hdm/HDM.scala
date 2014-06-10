package org.nicta.wdy.hdm


/**
 * Created by Tiantian on 2014/5/23.
 *
 * HDM : Hierarchy Distributed Matrices
 */
trait HDM[+T] extends Matrices[T] {

  def children: List[HDM[T]]

  def distribution: Distribution

  def location: Location

  def isLeaf: Boolean

  def elements: List[HDM[T]] = children

  def map[U](f: T => U): HDM[U] = this.apply(f)

  def flatMap[U](f: T => U): HDM[U]

  def collect():HDM[T]

  def shuffle(partitioner: Partitioner):HDM[T]

  def apply[T, U](f: T => U): HDM[U] = {
    HDM(children.map(_.apply(f)).toArray)
  }

  override def apply[B >: T](m: Int, n: Int): B = ???

  override def cExtract[B >: T](from: Long, to: Long): Matrices[B] = ???

  override def rExtract[B >: T](from: Long, to: Long): Matrices[B] = ???

  override def lAppend[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def append[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def length(): Unit = ???

  override def inverse(): Unit = ???

  override def t[B >: T](): Matrices[B] = ???

  override def /[B >: T](m: B): Matrices[B] = ???

  override def /[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def *[B >: T](m: B): Matrices[B] = ???

  override def *[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def -[B >: T](m: B): Matrices[B] = ???

  override def -[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def +[B >: T](m: B): Matrices[B] = ???

  override def +[B >: T](m: Matrices[B]): Matrices[B] = ???

  override def cLength(): Long = ???

  override def rlength(): Long = ???

}

trait DoubleHDM extends HDM[Double]



object DoubleHDM {

  def apply(elems: Array[Double]): HDM[Double] = {
    new LeafValHDM(elems)
  }

  def apply(elems: List[Double]): HDM[Double] = {
    new LeafValHDM(elems)
  }

  def apply(elems: Array[HDM[Double]]): HDM[Double] = {
    new ComplexHDM[Double](elems)
  }

  /*
  def apply(elems: List[HDM[Double]]): HDM[Double] = {
    new ComplexHDM[Double](elems)
  }
  */

  def apply(elemPath: String): HDM[Double] = {
    new RemoteHDM[Double](elemPath)
  }
}


object HDM{

  def apply(elems: Array[Double]): HDM[Double] = {
    new LeafValHDM(elems)
  }

  def apply[T](elems: List[T]): HDM[T] = ???

  def apply[T](elems: Array[HDM[T]]): HDM[T] = {
    new ComplexHDM[T](elems)
  }

/*  def apply[T](elems: List[HDM[T]]): HDM[T] = {
    new ComplexHDM[T](elems)
  }*/

  def apply[T](elemPath: String): HDM[T] = {
    new RemoteHDM[T](elemPath)
  }

  def findRemoteHDM[T](path:String): List[HDM[T]] = ???
}


sealed trait Distribution extends Serializable

case object Vertical extends Distribution

case object Horizontal extends Distribution


sealed trait Location extends Serializable

case object Local extends Location

case object Remote extends Location