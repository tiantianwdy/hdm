package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import org.nicta.wdy.hdm.functions.{ParallelFunction, DDMFunction_1, SerializableFunction}
import java.util.concurrent.Callable
import scala.concurrent.{CanAwait, ExecutionContext}
import scala.reflect.runtime.universe._
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager, BlockManager, Block}

/**
 * Created by Tiantian on 2014/12/1.
 */
case class Task[I:TypeTag,R: TypeTag](appId:String,
                     taskId:String,
                     input:Seq[HDM[_, I]],
                     func:ParallelFunction[I,R])
                     extends Serializable with Callable[Seq[DDM[R]]]{


  override def call() = try {
    //load input data
    val inputData = input.flatMap(b => HDMBlockManager.loadOrCompute[I](b.id).map(_.data))
    //apply function
    val data = func.apply(inputData)
    //partition as seq of data
    // map to blocks
    val blocks = Seq(data)
    //output
    val ddms = DDM(blocks)
    ddms
//    DFM(children = input, blocks = blocks.map(_.id), state = Computed, func = func)
  } catch {
    case e : Throwable =>
      e.printStackTrace()
    throw e
  }
}
