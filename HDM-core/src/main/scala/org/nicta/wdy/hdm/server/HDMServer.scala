package org.nicta.wdy.hdm.server

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.nicta.wdy.hdm.executor.HDMContext

import scala.util.Try

/**
 * Created by Tiantian on 2014/12/19.
 */
object HDMServer {


  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  var defaultConf = ConfigFactory.load("hdm-core.conf")
  var parentPath = defaultConf.getString("hdm.cluster.master.url")
  var port = defaultConf.getInt("akka.remote.netty.tcp.port")
  var slots = 1
  var blockServerPort = 9091
  var mode = "single-cluster"
  var isMaster = Try {
    defaultConf.getBoolean("hdm.cluster.isMaster")
  } getOrElse false

  private def loadConf(conf: Config) {
    parentPath = conf.getString("hdm.cluster.master.url")
    port = conf.getInt("akka.remote.netty.tcp.port")
    isMaster = Try {
      defaultConf.getBoolean("hdm.cluster.isMaster")
    } getOrElse false
  }

  /**
   * server main startup
   * @param args {
   *             case "-m" | "-master" => is this a Master
                  case "-p" | "-port" => port of akka system
                  case "-P" | "-parent" => parentPath of this node
                  case "-c" | "-conf" => name of config
                  case "-f" | "-file" => file path of config
   *              }
   */
  def main(args: Array[String]) {

    val paramMap = args.toList.grouped(2).map {
      tokens: List[String] => (tokens(0), tokens(1))
    }

    paramMap.foreach(param => param._1 match {
      case "-p" | "-port" => port = param._2.toInt
      case "-b" | "-bport" => blockServerPort = param._2.toInt
      case "-m" | "-master" => isMaster = param._2.toBoolean
      case "-P" | "-parent" => parentPath = param._2
      case "-M" | "-mode" => mode = param._2
      case "-s" | "-slots" => slots = param._2.toInt
      case "-c" | "-conf" => try {
        defaultConf = ConfigFactory.load(param._2)
        loadConf(defaultConf)
      }
      case "-f" | "-file" => try {
        defaultConf = ConfigFactory.parseFile(new File(param._2))
        loadConf(defaultConf)
      }
    })

    val hDMContext = new HDMContext(defaultConf)

    if (isMaster)
      hDMContext.startAsMaster(port = port, conf = defaultConf, mode = mode)//port, defaultConf
    else
      hDMContext.startAsSlave(parentPath, port, blockServerPort, defaultConf, slots)//parentPath, port, defaultConf
    println(s"[HDM Node Startted] as ${if (isMaster) "master" else "slave"} at port: $port .")

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        hDMContext.shutdown()
      }
    })
  }


}
