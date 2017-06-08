package org.hdm.core.utils

import scala.sys.process.ProcessLogger

/**
  * Created by tiantian on 10/05/17.
  */
class DefaultProcessLogger extends ProcessLogger with Logging {

  override def out(s: => String): Unit = {
    log.info(s)
  }

  override def buffer[T](f: => T): T = f

  override def err(s: => String): Unit = log.error(s)
}
