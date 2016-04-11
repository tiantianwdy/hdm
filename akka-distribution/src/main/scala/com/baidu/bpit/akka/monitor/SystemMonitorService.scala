package com.baidu.bpit.akka.monitor

import scala.Array.canBuildFrom
import scala.io.Source


/**
 * 系统资源监控的方法类，提供CPU，内存，网络，JVM的资源数据访问方法
 * 目前只提供了Lilux的资源获取方法
 * wudongyao
 */
object SystemMonitorService {
  var lastCpuInfo: Array[Long] = Array.fill(8)(0)
  var lastCpuMonitorTime: Long = 0
  var lastNetInfo: Tuple2[Long, Long] = (0, 0)
  var lastNetMonitorTime: Long = 0

  def getCpuRate = {
    val curCpuInfo = LinuxSystemMonitor.getCpuInfo(8)
    val duration = System.currentTimeMillis() - lastCpuMonitorTime
    val cpuRate = 100D * LinuxSystemMonitor.getCpuUtilization(lastCpuInfo, curCpuInfo, duration)
    lastCpuMonitorTime = System.currentTimeMillis()
    lastCpuInfo = curCpuInfo
    cpuRate
  }

  def getMemInfo: Array[Long] = {
    LinuxSystemMonitor.getMemInfo
  }

  def getMemRate = {
    val memInfo = getMemInfo
    if (memInfo.length < 4)
      0.toDouble
    else
      100 - 100 * (memInfo(1) + memInfo(2) + memInfo(3)) / memInfo(0).toDouble
  }

  def getNetRate = {
    val source = Source.fromFile("/proc/net/dev")
    try {
      val lines = source.getLines().toArray[String]
      val values: Array[Long] = lines(1).split("[\\s:]+").map(
        _ match {
          case s: String if (s.matches("\\d+")) => s.toLong
          case _ => 0
        })
      val netInfo = (values(2) - lastNetInfo._1, values(10) - lastNetInfo._2)
      lastNetInfo = (values(2), values(10))
      netInfo
    } finally {
      source.close()
    }
  }

  def getJVMMemInfo = {
    val runtime = Runtime.getRuntime
    val maxMem = runtime.maxMemory
    val freeMem = runtime.freeMemory
    val totalHeapMem = runtime.totalMemory
    val usedMem = totalHeapMem - freeMem
    Seq(freeMem, usedMem, totalHeapMem, maxMem)
  }

  def getJVMMemRate = {
    val memInfo = getJVMMemInfo
    100 * (memInfo(1) / memInfo(3).toDouble)
  }

  def main(args: Array[String]) {
    val memInfo = getMemInfo
    getCpuRate
    getNetRate
    Thread.sleep(1000)
    println("Cpu Rate: %f".format(getCpuRate))
    println("Mem Rate: %f".format(getMemRate))
    println("Mem info: Mem total:%s Mem free:%s Swap total:%s Swap free:%s ".format(memInfo(0), memInfo(1), memInfo(2), memInfo(3)))
    println("Net Rate: %s".format(getNetRate.toString()))
  }

}