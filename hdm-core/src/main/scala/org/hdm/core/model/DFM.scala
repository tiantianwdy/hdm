package org.hdm.core.model

import org.hdm.core.context.{HDMContext, AppContext}
import org.hdm.core.executor._
import org.hdm.core.functions._
import org.hdm.core.io.Path

import scala.reflect.ClassTag

import org.hdm.core.storage.{Declared, BlockState, BlockRef}

/**
 * Created by Tiantian on 2014/5/25.
 */
case class DFM[T: ClassTag, R: ClassTag](val children: Seq[_ <: HDM[T]],
                                         val id: String = HDMContext.newClusterId(),
                                         val dependency: DataDependency = OneToOne,
                                         val func: ParallelFunction[T, R] = null,
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
                                         val appContext:AppContext) extends ParHDM[T, R]{




//  def this(elem: Array[_<:ParHDM[_,T]]){
//    this(elem.toSeq)
//  }


  override def andThen[U: ClassTag](hdm: ParHDM[R, U]): ParHDM[T, U] = {
    val dep = if(this.dependency == NToOne && hdm.dependency == OneToN) NToN
              else if (this.dependency == NToOne && hdm.dependency == OneToOne) NToOne
              else if (this.dependency == OneToOne && hdm.dependency == OneToN) OneToN
              else OneToOne
    new DFM(this.children,
      hdm.id,
      dep,
      this.func.andThen(hdm.func),
      blocks, distribution, location, null, blockSize, isCache,
      state, parallelism,
      this.keepPartition && hdm.keepPartition,
      hdm.partitioner, appContext )
  }


  override def copy(id: String = this.id,
           children: Seq[HDM[T]] = this.children,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = this.blocks,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           blockSize:Long = this.blockSize,
           isCache: Boolean = this.isCache,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):ParHDM[T,R] = {

    new DFM(children, id,
      dependency,
      func,
      blocks, distribution, location, preferLocation, blockSize,
      isCache, state, parallelism, keepPartition, partitioner, this.appContext)
  }


  }

