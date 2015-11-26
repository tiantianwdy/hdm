package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.functions.{TwoInputFunction, ParallelFunction}
import org.nicta.wdy.hdm.io.BufferedBlockIterator
import org.nicta.wdy.hdm.model.{DDM, DataDependency, HDM}

import scala.collection.mutable
import scala.reflect._

/**
 * Created by tiantian on 23/11/15.
 */
case class TwoInputTask[I: ClassTag, U: ClassTag, R: ClassTag](appId: String,
                                                               taskId: String,
                                                               input1: Seq[HDM[_, I]],
                                                               input2: Seq[HDM[_, U]],
                                                               func: TwoInputFunction[I, U, R],
                                                               dep: DataDependency,
                                                               keepPartition: Boolean = true,
                                                               partitioner: Partitioner[R] = null) extends ParallelTask[R] {


  final val inTypeOne = classTag[I]

  final val inTypeTwo = classTag[U]

  final val outType = classTag[R]

  override def input = input1 ++ input2

  override def call(): Seq[DDM[_, R]] = {
    val input1Iter = new BufferedBlockIterator[I](input1)
    val input2Iter = new BufferedBlockIterator[U](input2)
    val res = func.aggregate((input1Iter, input2Iter), mutable.Buffer.empty[R])
    val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, res))
    } else {
      partitioner.split(res).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms

  }

}
