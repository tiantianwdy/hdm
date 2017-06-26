package org.hdm.core.model

import org.hdm.core.context.HDMContext
import org.hdm.core.functions.{NullParFunc, ParallelFunction, Partitioner}
import org.hdm.core.io.Path
import org.hdm.core.storage.BlockState

import scala.reflect._
import scala.util.Try

/**
 * Created by tiantian on 25/04/16.
 */
abstract class ParHDM[T:ClassTag, R:ClassTag]() extends  HDM[R] {

  val inType = classTag[T]

  override val children: Seq[ _<: HDM[T]]

  override val func:ParallelFunction[T, R]

  override def andThen[U:ClassTag](hdm: ParHDM[R,U]):ParHDM[T, U]

  def flatMap[U](f: R => U): ParHDM[R,U] = ???

  def fold[B](t: B)(f: (B, R) => B): ParHDM[R,B] = ???

  def foldLeft[B](f: (B, R) => B)(implicit t: B): ParHDM[R,B] = fold(t) (f)

  def foldRight[B](t: B)(f: (R, B) => B): ParHDM[R,B]  = ???

  def foreach[U] (f:R => U): Unit = ???

  def foldByKey[K,B](z:B)(f: R=>K, r: (B,R) => B): ParHDM[K,(K,B)] = ???

  def tailRecursive(num:Int, f: ParHDM[_,R] => ParHDM[_,R]):ParHDM[_,R] = ???

  def repeat(num:Int, f: ParHDM[_,R] => Any): Unit = ???

  def repeatWhile(condition: (ParHDM[_,R], HDMContext) => Boolean, f: (ParHDM[T,R], HDMContext) => Any): Unit = ???

  // function alias

  def apply[U:ClassTag](f: R => U): ParHDM[R, U] = map(f)


  def copy(id: String = this.id,
           children:Seq[HDM[T]]= this.children,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = null,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           blockSize:Long = this.blockSize,
           isCache: Boolean = this.isCache,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):ParHDM[T,R]


  override def toString: String = {
    s"HDM:{\n"+
      s"class:[${super.toString}] \n"+
      s"id:$id \n"+
      s"dep:$dependency \n"+
      s"location:${location} \n"+
      s"func:${func} \n" +
      s"block size:${blockSize} \n" +
      s"blocks:${blocks} \n" +
      s"parallelim:${parallelism} \n" +
      s"partitionNum:${if(partitioner ne null) partitioner.partitionNum else "" } \n" +
      s"children:[${if(children ne null) Try {children.map(_.id).mkString(" , ")} else "" }] \n" +
      s"state: ${state} \n" +
      s"cache: ${isCache} \n" +
      "}"
  }

  override def toSerializable(): HDM[R] = {
    this.copy(children = Seq.empty[HDM[T]], func = new NullParFunc[T, R])
  }
}



