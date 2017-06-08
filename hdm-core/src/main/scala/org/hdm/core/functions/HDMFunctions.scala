package org.hdm.core.functions

import org.hdm.core.executor.Partitioner
import org.hdm.core.model.DataDependency

/**
 * Created by tiantian on 8/01/15.
 */
trait HDMFunctions {

  val denpendency: DataDependency

  val func: ParallelFunction[_,_]

  val partitioner: Partitioner[_]


}
