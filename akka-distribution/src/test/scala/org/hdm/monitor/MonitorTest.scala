package org.hdm.akka.monitor

import junit.framework.TestCase

class MonitorTest extends TestCase{

  
  def testJVMMonitor() {
	  SystemMonitorService.getJVMMemInfo.foreach(println)
	  println("jvmMemRate:" + SystemMonitorService.getJVMMemRate)
    for(i <- 0 until 10)
    {
      Thread.sleep(2000);
      println(SystemMonitorService.getCpuRate)
      println(LinuxSystemMonitor.getCpuInfo(8).mkString(" , "))
    }
  }
  
  
}