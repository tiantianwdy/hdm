package org.nicta.wdy.hdm.scheduling

import org.apache.curator.utils.PathUtils
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable

/**
 * Created by tiantian on 20/07/16.
 */
/**
 *
 * @param processDistance the distance for process local address
 * @param nodeDistance the distance for node local address
 * @param rackDistance the distance for rack local address
 */
class DelayScheduling(processDistance:Long, nodeDistance:Long, rackDistance:Long) extends SchedulingPolicy with  Logging {

  @inline
  def checkLocality(nodeAddr:Path, inputAddr:Path): Int ={
//    println(s"nodeAddr:${nodeAddr}, inputAddr:${inputAddr}")
    require(nodeAddr != null && inputAddr != null)
    val distance = Path.calculateDistance(nodeAddr, inputAddr)
    val locality = if(distance <= processDistance) DelayScheduling.PROCESS_LOCAL
    else if (distance <= nodeDistance) DelayScheduling.NODE_LOCAL
    else if (distance <= rackDistance) DelayScheduling.RACK_LOCAL
    else DelayScheduling.ANY
    locality
  }


  /**
   *
   * @param inputs
   * @param resources
   * @param computeFactor reflects the time factor of computing a unit of data, normally computeFactor >> ioFactor >= networkFactor
   * @param ioFactor      reflects the time factor of loading a unit of data from local node
   * @param networkFactor reflects the time factor of loading a unit of data from remote node through network
   * @return              pairs of planed tasks: (taskID -> workerPath)
   */
  override def plan(inputs: Seq[SchedulingTask], resources: Seq[Path], computeFactor: Double, ioFactor: Double, networkFactor: Double): mutable.Map[String, String] = {
    val taskBuffer = inputs.toBuffer
    val resBuffer = resources.toBuffer
    val results = mutable.Map.empty[String, String]


    for(curLocality <- DelayScheduling.PROCESS_LOCAL to DelayScheduling.ANY){
        val assignedRes = mutable.Buffer.empty[Path]
        if(taskBuffer.nonEmpty && resBuffer.nonEmpty){
        resBuffer.foreach { res =>
          taskBuffer.find(t => {
            (t.inputs.length == 1) && (checkLocality(res, t.inputs.head) == curLocality)
          }) match {
            case Some(task) =>
              taskBuffer -= task
              results += task.id -> res.toString
              assignedRes += res
              println(s"${task.id}, ${res.toString}")
            case None => // do nothing
          }
        }
      }
      resBuffer --= assignedRes
    }

    results
  }


}


object DelayScheduling {

  val PROCESS_LOCAL = 0
  val NODE_LOCAL = 1
  val RACK_LOCAL = 2
  val ANY = 3

}