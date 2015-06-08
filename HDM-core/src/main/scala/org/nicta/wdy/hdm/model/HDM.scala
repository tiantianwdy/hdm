package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.{Buf, ClosureCleaner}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.reflect.runtime.universe.{WeakTypeTag,weakTypeOf}
import scala.reflect.{classTag,ClassTag}

import org.nicta.wdy.hdm.executor._
import org.nicta.wdy.hdm.functions._
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.storage.{HDMBlockManager, BlockState, BlockRef}

import scala.util.Try

/**
 * Created by Tiantian on 2014/5/23.
 *
 * HDM : Hierarchy Distributed Matrices
 */
abstract class HDM[T:ClassTag, R:ClassTag] extends Serializable{


  val id :String

  val children: Seq[_<:HDM[_, T]]

  val dependency: DataDependency

  val func: ParallelFunction[T,R]

  val blocks: Seq[String]

  val distribution: Distribution

  val location: Path // todo change name to path

  val preferLocation:Path

  val state: BlockState

  var parallelism: Int

  val keepPartition:Boolean
  
  val partitioner: Partitioner[R]


  val inType = classTag[T]


  val outType = classTag[R]

  def elements:  Seq[_<:HDM[_, T]] = children

  //  functional operations

  def map[U:ClassTag](f: R => U): HDM[R,U] = {
    ClosureCleaner(f)
    new DFM[R,U](children = Seq(this), dependency = OneToOne, func = new ParMapFunc(f), distribution = distribution, location = location)
  }

  def reduce[R1>: R :ClassTag](t:R1)(f: (R1, R1) => R): HDM[_,R1] =  { //parallel func is different with aggregation func
    ClosureCleaner(f)
    val mapAllFunc = (elems:Buf[R]) => Buf(elems.reduce(f))
    val parallel = new DFM[R,R](children = Seq(this), dependency = OneToOne, func = new ParMapAllFunc[R,R](mapAllFunc), distribution = distribution, location = location)
    new DFM[R,R1](children = Seq(parallel), dependency = NToOne, func = new ParReduceFunc[R,R1](f), distribution = distribution, location = location).withParallelism(1)
  }

  def filter(f: R => Boolean)  = {
//    ClosureCleaner(f)
    new DFM[R,R](children = Seq(this), dependency = OneToOne, func = new ParFindByFunc(f), distribution = distribution, location = location)
  }

  def groupBy[K:ClassTag](f: R=> K): HDM[_,(K, Buf[R])] = {
    ClosureCleaner(f)

    val pFunc = (t:R) => f(t).hashCode()
    val parallel = new DFM[R,R](children = Seq(this), dependency = OneToN, func = new NullFunc[R], distribution = distribution, location = location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
//    val parallel = this.copy(dependency = OneToN, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
    new DFM[R,(K, Buf[R])](children = Seq(parallel), dependency = NToOne, func = new ParGroupByFunc(f), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[(K, Buf[R])](1))


/*
  val pFunc = (t:(K, Seq[R])) => t._1.hashCode()
    val parallel = new DFM[R,(K, Seq[R])](children = Seq(this), dependency = OneToN, func = new ParGroupByFunc(f), distribution = distribution, location = location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))

    val combineByKey = (elems:Seq[(K,Seq[R])]) => {
        val grp = elems.groupBy(_._1).toSeq
        for(g <- grp) yield {
          val k = g._1
          val value = ArrayBuffer.empty[R]
          for(seq <- g._2) value ++= seq._2
          (k,value)
        }
      }
    new DFM[(K, Seq[R]),(K, Seq[R])](children = Seq(parallel), dependency = NToOne, func = new ParMapAllFunc(combineByKey), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[(K, Seq[R])](1))
*/


  }


  def groupReduce[K:ClassTag](f: R=>K, r: (R, R) => R): HDM[_, (K,R)] = {
    ClosureCleaner(f)
    ClosureCleaner(r)
    val pFunc = (t:(K, R)) => t._1.hashCode()
    val parallel = new DFM[R,(K, R)](children = Seq(this), dependency = OneToN, func = new ParReduceBy(f, r), distribution = distribution, location = location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
//    val groupReduce = (elems:Seq[(K,R)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toSeq
    new DFM[(K, R),(K, R)](children = Seq(parallel), dependency = NToOne, func = new ReduceByKey(r), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[(K, R)](1))

  }

  def groupReduceByKey[K:ClassTag](f: R=>K, r: (R, R) => R): HDM[_, (K,R)] = {
    ClosureCleaner(f)
    ClosureCleaner(r)
    val pFunc = (t:(K, R)) => t._1.hashCode()
    val mapAll = (elems:Buf[R]) => {
      elems.groupBy(f).mapValues(_.reduce(r)).toBuffer
    }
    val parallel = new DFM[R,(K, R)](children = Seq(this), dependency = OneToN, func = new ParMapAllFunc(mapAll), distribution = distribution, location = location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
    val groupReduce = (elems:Buf[(K,R)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toBuffer
    new DFM[(K, R),(K, R)](children = Seq(parallel), dependency = NToOne, func = new ParMapAllFunc(groupReduce), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[(K, R)](1))

  }

  def groupMapReduce[K:ClassTag, V:ClassTag](f: R=>K, m: R=>V, r: (V, V) => V): HDM[_, (K,V)] = {
    ClosureCleaner(f)
    ClosureCleaner(m)
    ClosureCleaner(r)
    val pFunc = (t:(K, V)) => t._1.hashCode()
    val mapAll = (elems:Buf[R]) => {
      elems.groupBy(f).mapValues(_.map(m).reduce(r)).toBuffer
    }
    val parallel = new DFM[R,(K, V)](children = Seq(this), dependency = OneToN, func = new ParMapAllFunc(mapAll), distribution = distribution, location = location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
    val groupReduce = (elems:Buf[(K,V)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toBuffer
    new DFM(children = Seq(parallel), dependency = NToOne, func = new ParMapAllFunc(groupReduce), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def count(): HDM[_, Int] = {
    val countFunc = (elems:Buf[R]) =>  Buf(elems.size)
    val parallel = new DFM[R,Int](children = Seq(this), dependency = OneToOne, func = new ParMapAllFunc(countFunc), distribution = distribution, location = location, keepPartition = false, partitioner = new KeepPartitioner[Int](1))
    val reduceFunc = (l1:Int, l2 :Int) => l1 + l2
    new DFM[Int,Int](children = Seq(parallel), dependency = NToOne, func = new ParReduceFunc(reduceFunc), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[Int](1), parallelism = 1)

  }

  def top(k:Int)(implicit ordering: Ordering[R]): HDM[_, R] = {
    val topFunc = (elems:Buf[R]) =>  elems.sorted(ord = ordering.reverse)
    val parallel = new DFM[R, R](children = Seq(this), dependency = OneToOne, func = new ParMapAllFunc(topFunc), distribution = distribution, location = location, keepPartition = false, partitioner = new KeepPartitioner[R](1))
    new DFM[R,R](children = Seq(parallel), dependency = NToOne, func = new ParMapAllFunc(topFunc), distribution = distribution, location = location, keepPartition = true, partitioner = new KeepPartitioner[R](1), parallelism = 1)

  }

  def flatMap[U](f: R => U): HDM[R,U] = ???

  def fold[B](t: B)(f: (B, R) => B): HDM[R,B] = ???

  def foldLeft[B](f: (B, R) => B)(implicit t: B): HDM[R,B] = fold(t) (f)

  def foldRight[B](t: B)(f: (R, B) => B): HDM[R,B]  = ???

  def foreach[U] (f:R => U): Unit = ???

  def foldByKey[K,B](z:B)(f: R=>K, r: (B,R) => B): HDM[K,(K,B)] = ???

  // function alias

  def apply[U:ClassTag](f: R => U): HDM[R, U] = map(f)


  // constructing operations

  def union[A <: R](h:HDM[_, A]): HDM[R,R]  = {

    new DFM[R,R](children = Seq(this,h.asInstanceOf[HDM[_, R]]), dependency = NToOne, func = new ParUnionFunc[R] , distribution = distribution, location = location)
  }

  def distinct[A <: R](h:HDM[_, A]): HDM[R,R]   = ???

  def intersection[A <: R](h:HDM[_, A]): HDM[R,R]   = ???

  def shuffle(partitioner: Partitioner[R]):HDM[R,R]    = ???

  def collect():Future[Iterator[R]] = ???

  def withPartitioner(partitioner: Partitioner[R]):HDM[T,R] = ???

  def withParallelism(p:Int):HDM[T,R] = {
    this.parallelism = p
    this
  }

  def withPartitionNum(p:Int):HDM[T,R] = {
    if(partitioner != null) partitioner.partitionNum = p
    this
  }

  def toURL = location.toString


  def compute(parallelism:Int):Future[HDM[_, _]]  =  HDMContext.compute(this, parallelism)


  def copy(id: String = this.id,
           children:Seq[HDM[_, T]]= this.children,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = null,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):HDM[T,R]


  def andThen[U:ClassTag](hdm: HDM[R,U]):HDM[T,U]


  def sample(size: Int = 10): Seq[String] = {
    // todo change to distributed version
    val ddms = blocks.map( url =>
      HDMBlockManager().getBlock(Path(url).name))
    if(ddms != null && !ddms.isEmpty)
      //todo ask blocks from remote block manager
      ddms.map(_.data.asInstanceOf[mutable.Buffer[R]]).flatten.take(size).map(_.toString)
    else Seq.empty[String]
  }



  override def toString: String = {
    s"HDM:{\n"+
      s"class:[${super.toString}] \n"+
      s"id:$id \n"+
      s"dep:$dependency \n"+
      s"location:${location} \n"+
      s"func:${func} \n" +
      s"blocks:${blocks} \n" +
      s"parallelim:${parallelism} \n" +
      s"partitionNum:${if(partitioner ne null) partitioner.partitionNum else "" } \n" +
      s"children:[${if(children ne null) Try {children.map(_.id).mkString(" , ")} else "" }] \n" +
      "}"
  }

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

  def apply[T:ClassTag](elems: Array[T]): DDM[_,T] = {
    DDM(elems)
  }

  def apply(path: Path, keepPartition: Boolean = false): DFM[_, String] = {
    new DFM(children = null, location = path, keepPartition = keepPartition, func = new NullFunc[String])
  }

  def horizontal[T:ClassTag](elems: Array[T]*) : HDM[_,T] = {
    val children = elems.map(e => DDM(e))
    new DFM(children = children, func = new ParUnionFunc[T], distribution = Horizontal, parallelism = 1)
  }

  def parallel[T:ClassTag](elems: Seq[T], split: Int = HDMContext.CORES): HDM[_,T] = {
    val ddms = new RandomPartitioner[T](split).split(elems).map(d => DDM(d._2))
    new DFM(children= ddms.toSeq, func = new ParUnionFunc[T], distribution = Horizontal)
  }

  def horizontal[T:ClassTag](paths: Array[Path], func: String => T) : HDM[Path, T] = ???

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

  def findRemoteHDM[T:ClassTag, R:ClassTag](path:String): List[HDM[T,R]] = ???
}


sealed trait Distribution extends Serializable

case object Vertical extends Distribution

case object Horizontal extends Distribution


sealed trait Location extends Serializable

case object Local extends Location

case object Remote extends Location