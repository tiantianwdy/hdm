package org.nicta.wdy.hdm.utils

import org.slf4j.{LoggerFactory, Logger}

/**
 * Created by tiantian on 30/04/15.
 */
trait Logging {

  @transient
  private var _log: Logger = null

  protected def log: Logger = {
    if(_log eq null){
      var className = this.getClass.getName
      if(className.endsWith("$")){
        className = className.substring(0,className.length -1)
      }
      _log = LoggerFactory.getLogger(className)
    }
    _log
  }

}
