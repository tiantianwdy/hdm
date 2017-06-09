package org.hdm.core.executor


import java.lang.{ProcessBuilder => JProcess}

import org.junit.Test
/**
  * Created by tiantian on 10/05/17.
  */
class ExecutorLauncherTest {

  val launcher = new ExecutorLauncher

  @Test
  def testCmd(): Unit ={
    val hdmHome = "/home/tiantian/Dev/lib/hdm/hdm-core"
    val stream = launcher.execute(s"find $hdmHome/lib -name *.jar | xargs ")
    stream.foreach(println(_))
  }

  @Test
  def testMannualStartWorker(): Unit ={
    val hdmHome = "/home/tiantian/Dev/lib/hdm/hdm-core"
    val masterAddress = "127.0.0.1:8998"
    val masterPath = s"akka.tcp://masterSys@${masterAddress}/user/smsMaster"
    val slavePort = 10001
    val blockServerPort = 9001
    val numOfCores = 2
    val mem = "256M"
    val xmem = "256M"
    val libs = launcher.call(s"find $hdmHome/lib -name *.jar | xargs ")
    println(libs)
    val cmd = s"""java -Xms$mem  -Xmx$xmem -Dfile.encoding=UTF-8 -jar $hdmHome/hdm-core-0.0.1.jar -P $masterPath -m false -p $slavePort  -s $numOfCores -b $blockServerPort"""
    val process = launcher.launchProcess(cmd, "hdm-executor-test.log")
    Thread.sleep(10000)
    process.destroy()
//    val status = process.exitValue()
//    println(s"Exit executor with :$status")
  }

  @Test
  def testLaunchSlave(): Unit ={
    val master = "127.0.1.1:8999"
    val slavePort = 10001
    val blockServerPort = 9001
    val numOfCores = 2
    val mem = "3G"
    val HDM_HOME = "~/Dev/lib/hdm/hdm-core"

    val cmd = s"$HDM_HOME/startup.sh slave $slavePort $master $numOfCores $mem $blockServerPort"
    val process = launcher.launch(cmd)
    Thread.sleep(10000)
    process.destroy()
  }

  @Test
  def testLaunchMaster(): Unit ={
    val res = launcher.launchAppMaster("~/Dev/lib/hdm/hdm-core", "default#0.0.1", "127.0.1.1", 8998)
    println(res._1)
    Thread.sleep(10000)
    res._2.destroy()
//    res._2.wait()
  }

  @Test
  def testLaunchExecutor(): Unit ={
    val HDM_HOME = "/home/tiantian/Dev/lib/hdm/hdm-core"
    val masterAddress = "127.0.0.1:8998"
    val masterPath = s"akka.tcp://masterSys@${masterAddress}/user/smsMaster"
    val slavePort = 10001
    val blockServerPort = 9001
    val numOfCores = 2
    val mem = "3G"

    val process = launcher.launchExecutor(HDM_HOME, masterPath, "127.0.0.1", slavePort, numOfCores, mem, blockServerPort)
    Thread.sleep(10000)
    process.destroy()
  }

}
