package org.hdm

import _root_.akka.event.LoggingAdapter


/**
 *
 * @author wudongyao
 * @date 14-1-15,下午10:10
 * @version
 */
package object akka {


  def logEx (logger: LoggingAdapter, source: Any): PartialFunction[Throwable, Unit] = {
    case ex: Throwable => logger.error(ex, s"Exception at $source")
  }

}
