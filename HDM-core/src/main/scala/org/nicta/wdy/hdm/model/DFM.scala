package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm._
import org.nicta.wdy.hdm.executor.{KeepPartitioner, HDMContext, ClusterExecutorContext, Partitioner}
import org.nicta.wdy.hdm.functions._
import org.nicta.wdy.hdm.io.Path

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext
import org.nicta.wdy.hdm.storage.{Declared, BlockState, BlockRef}
import java.util.UUID

/**
 * Created by Tiantian on 2014/5/25.
 */
class DFM[T: ClassTag, R: ClassTag](val children: Seq[_ <: HDM[_, T]],
                                       val id: String = HDMContext.newClusterId(),
                                       val dependency: Dependency = OneToOne,
                                       val func: ParallelFunction[T, R] = null,
                                       val blocks: Seq[String] = null,
                                       val distribution: Distribution = Horizontal,
                                       val location: Path = Path(HDMContext.clusterBlockPath),
                                       val state: BlockState = Declared,
                                       var parallelism: Int = -1, // undefined
                                       val keepPartition:Boolean = true,
                                       val partitioner: Partitioner[R] = new KeepPartitioner[R](1)) extends HDM[T, R] {




  def this(elem: Array[_<:HDM[_,T]]){
    this(elem.toSeq)
  }

  def copy(id: String = this.id,
           children: Seq[HDM[_, T]] = this.children,
           dependency: Dependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = this.blocks,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):HDM[T,R] = {

    new DFM(children, id, dependency, func, blocks, distribution, location, state, parallelism, keepPartition, partitioner )
  }


  /*  override def reduce[A >: R](t: A)(f: (A, R) => A): HDM[R,A] = {

      new DFM[R,A](Seq(this), NToOne, new ReduceFunc[R,A](f), dis, loc)

  /*    val newElems = elems.map { hdm =>
        val nh = hdm.location match {
          case Local =>
            hdm.reduce(t)(f)
          case Remote =>
            // todo
            // find remote hdm address
            // send remote function and add wait events
            hdm.reduce(t)(f)
          case _ =>
            hdm.reduce(t)(f)
        }
        nh.children.head
      }
      // after all the elements have been computed
      newElems.reduce{ (h1,h2) =>
        h1.union(h2)
      }.reduce(t)(f)*/
    }*/

/*  override def groupBy[K](f: (R) => K): HDM[R,(K, Seq[R])] = {

    new DFM[R,(K, Seq[R])](Seq(this), NToOne, new GroupByFunc(f), dis, loc)
    /*val newElems = elems.map{ hdm =>
      val nh = hdm.location match {
        case Local =>
          hdm.groupBy(f)
        case Remote =>
          // todo
          // find remote hdm address
          // send remote function and add wait events
          hdm.groupBy(f)
        case _ =>
          hdm.groupBy(f)
      }
      nh
    }
    // aggregate
    newElems.reduce{ (h1,h2) =>
      h1.union(h2)
    }.groupBy(_._1).map(t => t._1 -> t._2.flatMap(_._2))*/
  }*/


  /*  override def apply[AnyVal, U](f: (AnyVal) => U): HDM[U] = {
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
    }*/


//  override def union[A <: R](h: HDM[_, A]): HDM[R, R] = {
//    new DFM[R,R](Seq(this,h.asInstanceOf[HDM[_, R]]), NToOne, new UnionFunc[R] , dis, location)
//  }
  }

