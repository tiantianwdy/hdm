package org.hdm.core.model

import org.hdm.core.context.{HDMContext, AppContext}
import org.hdm.core.executor.{KeepPartitioner, Partitioner}
import org.hdm.core.functions.{NullDualFunc, DualInputFunction, ParallelFunction}
import org.hdm.core.io.Path
import org.hdm.core.storage.{Declared, BlockState}

import scala.reflect._
import scala.util.Try

/**
 * Created by tiantian on 28/03/16.
 */
class DualDFM[T: ClassTag, U: ClassTag, R: ClassTag](val id: String = HDMContext.newClusterId(),
                                                     val input1:Seq[_ <: HDM[T]],
                                                     val input2:Seq[_ <: HDM[U]],
                                                     val dependency: DataDependency = NToOne,
                                                     val func: DualInputFunction[T, U, R] = null, //todo be changed to DualInputFunction
                                                     val blocks: Seq[String] = null,
                                                     val distribution: Distribution = Horizontal,
                                                     val location: Path,
                                                     val preferLocation:Path = null,
                                                     var blockSize:Long = -1,
                                                     var isCache:Boolean = false,
                                                     val state: BlockState = Declared,
                                                     var parallelism: Int = -1, // undefined
                                                     val keepPartition:Boolean = true,
                                                     val partitioner: Partitioner[R] = new KeepPartitioner[R](1),
                                                     val appContext: AppContext) extends HDM[R]{

  val inType1 = classTag[T]

  val inType2 = classTag[U]


  override val children: Seq[_ <: HDM[_]] = input1 ++ input2

  override def andThen[V: ClassTag](hdm: ParHDM[R, V]): HDM[V] = {
    val dep = if(this.dependency == NToOne && hdm.dependency == OneToN) NToN
    else if (this.dependency == NToOne && hdm.dependency == OneToOne) NToOne
    else if (this.dependency == OneToOne && hdm.dependency == OneToN) OneToN
    else OneToOne
    new DualDFM(id, input1, input2, dep,
      this.func.andThen(hdm.func),
      blocks, distribution, location, null, blockSize, isCache,
      state, parallelism,
      this.keepPartition && hdm.keepPartition,
      hdm.partitioner, this.appContext)
  }

  def copy(id: String = this.id,
           input1:Seq[_ <: HDM[T]] = this.input1,
           input2:Seq[_ <: HDM[U]] = this.input2,
           dependency: DataDependency = this.dependency,
           func: DualInputFunction[T, U, R] = this.func,
           blocks: Seq[String] = this.blocks,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation: Path = this.preferLocation,
           blockSize: Long = this.blockSize,
           isCache: Boolean = this.isCache,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner): DualDFM[T, U, R] = {

    new DualDFM[T,U,R](id,
      input1,
      input2,
      dependency,
      func,
      blocks,
      distribution,
      location,
      preferLocation,
      blockSize,
      isCache,
      state,
      parallelism,
      keepPartition,
      partitioner,
      this.appContext)
  }

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
      s"input1:[${if(input1 ne null) Try {input1.map(_.id).mkString(" , ")} else "" }] \n" +
      s"input2:[${if(input2 ne null) Try {input2.map(_.id).mkString(" , ")} else "" }] \n" +
      s"state: ${state} \n" +
      s"cache: ${isCache} \n" +
      "}"
  }

  override def toSerializable(): HDM[R] = {
    this.copy(input1 = Seq.empty[HDM[T]], input2 = Seq.empty[HDM[U]], func = new NullDualFunc[T, U, R])
  }
}
