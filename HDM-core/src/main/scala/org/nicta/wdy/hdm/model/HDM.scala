package org.nicta.wdy.hdm.model

import java.util.UUID

import scala.concurrent.Future
import scala.reflect.runtime.universe._

import org.nicta.wdy.hdm.executor.{RandomPartitioner, HDMContext, Partitioner}
import org.nicta.wdy.hdm.functions._
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.storage.{HDMBlockManager, BlockState, BlockRef}

/**
 * Created by Tiantian on 2014/5/23.
 *
 * HDM : Hierarchy Distributed Matrices
 */
abstract class HDM[T:TypeTag, R:TypeTag] {

  val id:String

  val children: Seq[_<:HDM[_, T]]

  val dependency: Dependency

  val func: ParallelFunction[T,R]

  val blocks: Seq[String]

  val distribution: Distribution

  val location: Path

  val state: BlockState

  final val inType = typeOf[T]

  final val outType = typeOf[R]

  def elements:  Seq[_<:HDM[_, T]] = children

  //  functional operations

  def map[U:TypeTag](f: R => U): HDM[R,U] = {

    DFM[R,U](children = Seq(this), dependency = OneToOne, func = new ParMapFunc(f), distribution = distribution, location = location)
  }

  def reduce[R1>: R :TypeTag](t:R1)(f: (R1, R1) => R): HDM[R,R1] =  {

    DFM[R,R1](children = Seq(this), dependency = NToOne, func = new ParReduceFunc[R,R1](f), distribution = distribution, location = location)

  }

  def groupBy[K:TypeTag](f: R=> K): HDM[R,(K,Seq[R])] = {

    DFM[R,(K, Seq[R])](children = Seq(this), dependency = NToOne, func = new ParGroupByFunc(f), distribution = distribution, location = location)

  }

  def reduceByKey[K:TypeTag](f: R=>K, r: (R, R) => R): HDM[R, (K,R)] = {

    DFM[R,(K, R)](children = Seq(this), dependency = NToOne, func = new ParReduceByKey(f, r), distribution = distribution, location = location)
  }

  def flatMap[U](f: R => U): HDM[R,U] = ???

  def fold[B](t: B)(f: (B, R) => B): HDM[R,B] = ???

  def foldLeft[B](f: (B, R) => B)(implicit t: B): HDM[R,B] = fold(t) (f)

  def foldRight[B](t: B)(f: (R, B) => B): HDM[R,B]  = ???

  def foreach[U] (f:R => U): Unit = ???

  def foldByKey[K,B](z:B)(f: R=>K, r: (B,R) => B): HDM[K,(K,B)] = ???

  // function alias

  def apply[U:TypeTag](f: R => U): HDM[R, U] = map(f)


  // constructing operations

  def union[A <: R](h:HDM[_, A]): HDM[R,R]  = {

    DFM[R,R](children = Seq(this,h.asInstanceOf[HDM[_, R]]), dependency = NToOne, func = new ParUnionFunc[R] , distribution = distribution, location = location)
  }

  def distinct[A <: R](h:HDM[_, A]): HDM[R,R]   = ???

  def intersection[A <: R](h:HDM[_, A]): HDM[R,R]   = ???

  def shuffle(partitioner: Partitioner):HDM[R,R]    = ???

  def compute():Future[HDM[_, _]]  =  HDMContext.compute(this)

  def collect():Future[Iterator[R]] = ???

  def sample(size: Int = 10): Seq[String] = {
    // change to distributed version
    val ddms = blocks.map( url =>
      HDMBlockManager().getBlock(Path(url).name))
    if(ddms != null && !ddms.isEmpty)
      ddms.map(_.data).flatten.take(size).map(_.toString)
    else Seq.empty[String]
  }

  def toURL = location.toString + "/" + id



/*  def apply[T, U](f: T => U): HDM[U] = {
    HDM(children.map(_.apply(f)).toArray)
  }*/

 // matrix operations
 /*
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

  */

  override def toString: String = {
   s"HDM:{\n"+
   s"class:[${super.toString}] \n"+
   s"id:$id \n"+
   s"dep:$dependency \n"+
   s"location:${location.toString} \n"+
   s"func:${func} \n" +
   "}"
 }
}

trait DoubleHDM extends HDM[Double,Double]



object DoubleHDM {

/*  def apply(elems: Array[Double]): HDM[Double] = {
    new LeafValHDM(elems)
  }

  def apply(elems: List[Double]): HDM[Double] = {
    new LeafValHDM(elems)
  }

  def apply(elems: Array[HDM[Double]]): HDM[Double] = {
    new ComplexHDM[Double](elems)
  }*/

  /*
  def apply(elems: List[HDM[Double]]): HDM[Double] = {
    new ComplexHDM[Double](elems)
  }
  */

  /*def apply(elemPath: String): HDM[Double] = {
    new RemoteHDM[Double](elemPath)
  }*/
}

/**
 * HDM data factory
 */
object HDM{

  def apply[T:TypeTag](elems: Array[T]): DDM[T] = {
    DDM(elems)
  }

  def apply(path: Path): DFM[Path, String] = {
    DFM[Path,String](children = null, location = path)
  }

  def horizontal[T:TypeTag](elems: Array[T]*) : HDM[_,T] = {
    DFM(children = elems.map(e => DDM(e)), func = new ParUnionFunc[T], distribution = Horizontal)
  }

  def parallel[T:TypeTag](elems: Seq[T], split: Int = HDMContext.CORES): HDM[_,T] = {
    val ddms = new RandomPartitioner().split[T](elems, split).map(d => DDM(d._2))
    DFM(children= ddms.toSeq, func = new ParUnionFunc[T], distribution = Horizontal)
  }

  def horizontal[T:TypeTag](paths: Array[Path], func: String => T) : HDM[Path, T] = ???

  /* def apply(elems: Array[Double]): HDM[Double] = {
     new LeafValHDM(elems)
   }

   def apply[T](elems: List[T]): HDM[T] = ???

   def apply[T](elems: Array[HDM[T]]): HDM[T] = {
     new ComplexHDM[T](elems)
   }*/

/*  def apply[T](elems: List[HDM[T]]): HDM[T] = {
    new ComplexHDM[T](elems)
  }*/

 /* def apply[T](elemPath: String): HDM[T] = {
    new RemoteHDM[T](elemPath)
  }*/

  def findRemoteHDM[T:TypeTag, R:TypeTag](path:String): List[HDM[T,R]] = ???
}


sealed trait Distribution extends Serializable

case object Vertical extends Distribution

case object Horizontal extends Distribution


sealed trait Location extends Serializable

case object Local extends Location

case object Remote extends Location