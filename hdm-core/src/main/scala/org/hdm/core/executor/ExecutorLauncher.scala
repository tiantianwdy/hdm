package org.hdm.core.executor

import java.io.{InputStreamReader, BufferedReader, File}

import org.hdm.core.utils.{Logging, DefaultProcessLogger}
import java.lang.{ProcessBuilder => JProcess}

import scala.collection.JavaConversions._

/**
  * Created by tiantian on 10/05/17.
  */
class ExecutorLauncher extends Logging {

  import scala.sys.process._


  def launch(path:String, script:String) = {
    Process("sh", Seq("-c", s"$path/$script")).run(new DefaultProcessLogger)

  }

  def launch(cmd :String) = {
    val scripts = cmd.split("\\|")
    val processPipeline = scripts.map{ s=>
      Process("sh", Seq("-c", s))
    }.reduceLeft(_ #| _)
    processPipeline.run(new DefaultProcessLogger)
  }


  def execute(cmd: String):Stream[String] = {
    val scripts = cmd.split("\\|")
    val processPipeline = scripts.map{ s=>
      Process("sh", Seq("-c", s))
    }.reduceLeft(_ #| _)
    processPipeline.lines(new DefaultProcessLogger)
  }

  def call(cmd: String):String = {
    val scripts = cmd.split("\\|")
    val processPipeline = scripts.map{ s=>
      Process("sh", Seq("-c", s))
    }.reduceLeft(_ #| _)
    processPipeline.!!(new DefaultProcessLogger)
  }

  def execCmd(cmd:String): Process ={
    val process = Process("sh", Seq("-c", cmd))
    process.run(new DefaultProcessLogger)
  }

  def launchProcess(script:String, output:String) = {
    val pb = new JProcess(script.split("\\s+").toList)
    pb.redirectOutput(new File(output))
    val process = pb.start()
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
//        process.destroyForcibly()
      }
    }))
    process
  }

  /**
    *
    * @param appID id of the application
    * @param host  host of the master
    * @param port port of the master
    * @return path of initiated master
    */
  def launchAppMaster(hdmHome:String, appID:String, host:String, port:Int): (String, Process) = {
    val masterPath  = s"akka.tcp://masterSys@$host:$port/user/smsMaster/ClusterExecutor"
    val cmd = s"$hdmHome/startup-master.sh -h $host -p $port"
    val process = launch(cmd)
    (masterPath, process)
  }


  def launchExecutor(hdmHome:String, master:String, host:String, port:Int, numOfCores:Int,  mem:String, blockServerPort:Int) ={
    val hdmVersion = HDMContext.HDM_VERSION
    val executorPath  = s"akka.tcp://slaveSys@$host:$port/user/smsSlave/ClusterExecutor"
    val cmd = s"""java -Xms$mem  -Xmx$mem -Dfile.encoding=UTF-8 -jar $hdmHome/hdm-core-$hdmVersion.jar -P $master -m false -p $port  -s $numOfCores -b $blockServerPort"""
    log.info(s"Launching executor with cmd: [$cmd].")
//    val cmd = s"$hdmHome/startup.sh slave $port $master $numOfCores $mem $blockServerPort"
    val process = launchProcess(cmd, "hdm-executor.log")
    process
  }

}


object ExecutorLauncher {

  def apply() ={
    new ExecutorLauncher
  }
}
