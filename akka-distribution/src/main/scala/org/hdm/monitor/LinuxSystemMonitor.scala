package org.hdm.akka.monitor

import java.io._
import java.util.StringTokenizer

/**
 * Created by Tiantian on 2014/5/26.
 */
object LinuxSystemMonitor {

  /**
   *
   * @return memory info
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @throws[IOException]
  @throws[InterruptedException]
  def getMemInfo(): Array[Long] = {
    val file = new File("/proc/meminfo")
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
    val result = Array.fill(6) {
      0L
    }
    try {
      var str = br.readLine()
      while (str != null) {
        val token = new StringTokenizer(str)
        if (token.hasMoreTokens()){
          str = token.nextToken()
          if (token.hasMoreTokens()){
            if (str.equalsIgnoreCase("MemTotal:"))
              result.update(0, token.nextToken().toLong)
            else if (str.equalsIgnoreCase("MemFree:"))
              result.update(1, token.nextToken().toLong)
            else if (str.equalsIgnoreCase("Buffers:"))
              result.update(2, token.nextToken().toLong)
            else if (str.equalsIgnoreCase("Cached:"))
              result.update(3, token.nextToken().toLong)
            else if (str.equalsIgnoreCase("SwapTotal:"))
              result.update(4, token.nextToken().toLong)
            else if (str.equalsIgnoreCase("SwapFree:"))
              result.update(5, token.nextToken().toLong)
            str = br.readLine()
          }
        }
      }
    } finally {
      if (br != null)
        br.close()
    }
    result
  }

  /**
   * get CPU utilization rate
   *
   * @param preCpuInfos
   * @param postCpuInfos
   * @param duration
	 * the length of duration for calculating the average CPU
   * utilization
   * @return float CPU utilization rate
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @throws[IOException]
  @throws[InterruptedException]
  def getCpuUtilization(preCpuInfos: Array[Long], postCpuInfos: Array[Long], duration: Long): Double = {
    val preTotalCpu = preCpuInfos.sum
    val postTotalCpu = postCpuInfos.sum
    val total = postTotalCpu - preTotalCpu
    val idle = postCpuInfos(3) - preCpuInfos(3)
    if (postTotalCpu - preTotalCpu != 0) {
      (total.toDouble - idle) / total
    } else 0D
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getCpuInfo(tokenLength: Int): Array[Long] = {
    val cpuInfo = Array.fill(tokenLength) {
      0L
    }
    val file = new File("/proc/stat")
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
    try {
      val token = new StringTokenizer(br.readLine())
      token.nextToken()
      for (i <- 0 until tokenLength if (token.hasMoreTokens())) {
        cpuInfo.update(i, token.nextToken().toLong)
      }
    } finally {
      if (br != null)
        br.close()
    }
    cpuInfo
  }

  @throws[IOException]
  @throws[InterruptedException]
  def getNetInfo(): Long = {
    val file = new File("/proc/net/dev")
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
    try {
      for (i <- 0 until 3)
      // skip to the correct line
        br.readLine()
      val token = new StringTokenizer(br.readLine())
      token.nextToken()
      val receiveByte = token.nextToken().toLong
      for (i <- 0 until 7)
      // skip to the transmit info
        token.nextToken()
      val transmitByte = token.nextToken().toLong
      (receiveByte + transmitByte) / 2
    } finally {
      if (br != null)
        br.close()
    }
  }

}
