package com.baidu.bpit.akka.monitor

import junit.framework.TestCase

class MonitorTest extends TestCase{

  
  def testJVMMonitor() {
	  SystemMonitorService.getJVMMemInfo.foreach(println)
	  println("jvmMemRate:" + SystemMonitorService.getJVMMemRate)
  }
  
  
}