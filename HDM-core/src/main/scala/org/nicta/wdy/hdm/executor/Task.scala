package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import org.nicta.wdy.hdm.functions.{ParallelFunction, DDMFunction_1, SerializableFunction}
import java.util.concurrent.Callable
import scala.concurrent.{CanAwait, ExecutionContext}
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager, BlockManager, Block}

/**
 * Created by Tiantian on 2014/12/1.
 */
case class Task[I:ClassTag,R: ClassTag](appId:String,
                     taskId:String,
                     input:Seq[HDM[_, I]],
                     func:ParallelFunction[I,R],
                     keepPartition:Boolean = true,
                     partitioner: Partitioner[R] = null,
                     createTime:Long = System.currentTimeMillis())
                     extends Serializable with Callable[Seq[DDM[R]]]{

  final val inType = classTag[I]

  final val outType = classTag[R]

  override def call() = try {
    //load input data
//    val inputData = input.flatMap(b => HDMBlockManager.loadOrCompute[I](b.id).map(_.data))
    val inputData = input.flatMap(hdm => hdm.blocks.map(Path(_))).map(p => HDMBlockManager().getBlock(p.name).data.asInstanceOf[Seq[I]])
    //apply function
    val data = func.apply(inputData.flatten)
    //partition as seq of data
    val ddms = if(partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
      Seq(DDM[R](taskId, data))
    } else {
      partitioner.split(data).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
    }
    ddms
//    DFM(children = input, blocks = blocks.map(_.id), state = Computed, func = func)
  } catch {
    case e : Throwable =>
      e.printStackTrace()
    throw e
  }
}
