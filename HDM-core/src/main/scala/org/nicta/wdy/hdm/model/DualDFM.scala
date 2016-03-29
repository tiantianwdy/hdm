package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.executor.{KeepPartitioner, Partitioner, HDMContext}
import org.nicta.wdy.hdm.functions.{DualInputFunction, ParallelFunction}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.storage.{Declared, BlockState}

import scala.reflect.ClassTag

/**
 * Created by tiantian on 28/03/16.
 */
class DualDFM[T: ClassTag, U: ClassTag, R: ClassTag](val id: String = HDMContext.newClusterId(),
                                                     val children: Seq[_ <: HDM[_, (T, U)]] = null,
                                                     val input1:Seq[_ <: HDM[_, T]],
                                                     val input2:Seq[_ <: HDM[_, U]],
                                                     val dependency: DataDependency = NToOne,
                                                     val func: DualInputFunction[T, U, R] = null, //todo be changed to DualInputFunction
                                                     val blocks: Seq[String] = null,
                                                     val distribution: Distribution = Horizontal,
                                                     val location: Path = Path(HDMContext.clusterBlockPath),
                                                     val preferLocation:Path = null,
                                                     var blockSize:Long = -1,
                                                     var isCache:Boolean = false,
                                                     val state: BlockState = Declared,
                                                     var parallelism: Int = -1, // undefined
                                                     val keepPartition:Boolean = true,
                                                     val partitioner: Partitioner[R] = new KeepPartitioner[R](1)) extends AbstractHDM[R]{


  override def andThen[V: ClassTag](hdm: HDM[R, V]): HDM[(T, U), V] = ???

  def copy(id: String,
           input1:Seq[_ <: HDM[_, T]],
           input2:Seq[_ <: HDM[_, U]],
           dependency: DataDependency,
           func: DualInputFunction[T, U, R],
           blocks: Seq[String],
           distribution: Distribution,
           location: Path,
           preferLocation: Path,
           blockSize: Long,
           isCache: Boolean,
           state: BlockState,
           parallelism: Int,
           keepPartition: Boolean,
           partitioner: Partitioner[R]): DualDFM[T, U, R] = {

    new DualDFM[T,U,R](id,
      this.children,
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
      partitioner)
  }
}
