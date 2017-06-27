package org.hdm.core.scheduling

import org.hdm.core.io.Path
import org.hdm.core.utils.Logging

import scala.collection.mutable

/**
 * Created by tiantian on 31/08/15.
 */
class OneByOneScheduling extends SchedulingPolicy with Logging{
  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data, normally computeFactor >> ioFactor >= networkFactor
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return
   */
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): mutable.Map[String, Path] = {
    val results = mutable.Map.empty[String, Path]
    inputs.map{ task =>
      task.id -> findPreferredWorker(task, resources)
    }.foreach { tuple =>
      results += tuple
    }
    results
  }

  private def findPreferredWorker(task: SchedulingTask, candidates:Seq[Path]): Path = try {

    log.debug(s"Block prefered input locations:${task.inputs.mkString(",")}")

    //find closest worker from candidates
    if (candidates.size > 0) {
      val workerPath = Path.findClosestLocation(candidates, task.inputs)
      workerPath
    } else Path("")
  } catch {
    case e: Throwable => log.error(s"failed to find worker for task:${task.id}"); Path("")
  }
}
