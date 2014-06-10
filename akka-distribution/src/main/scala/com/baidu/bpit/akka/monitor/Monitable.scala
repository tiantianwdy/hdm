package com.baidu.bpit.akka.monitor


/**
 * 
 * trait for monitors 
 * @author wudongyao
 * @date 2013-7-11 
 * @version 0.0.1
 *
 */
trait Monitable {
	
  /**
   *  this  method will be invoked by monitor extensions or collectors @see MonitorManager
   *  return sequence List of @see MonitorData
   */
  def  getMonitorData(reset:Boolean=true) : java.util.List[MonitorData]
  
}
