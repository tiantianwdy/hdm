package org.hdm.core.server

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.hdm.akka.server.SmsSystem
import org.hdm.core.context.{HDMServerContext, HDMContext}
import org.hdm.core.utils.Logging
import org.hdm.core.server.HDMEngine

import scala.util.Try

/**
 * Created by Tiantian on 2014/12/19.
 */
object HDMServer extends Logging {


  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  var defaultConf = ConfigFactory.load("hdm-core.conf")
  var clusterMaster = defaultConf.getString("hdm.cluster.master.url")
  var parentPath = defaultConf.getString("hdm.cluster.master.url")
  var port = defaultConf.getInt("akka.remote.netty.tcp.port")
  var host = defaultConf.getString("akka.remote.netty.tcp.hostname")
  var slots = 1
  var mem = "2G"
  var nodeType = "app" // or cluster
  var blockServerPort = 9091
  var mode = "single-cluster"
  var isMaster = Try {
    defaultConf.getBoolean("hdm.cluster.isMaster")
  } getOrElse false

  private def loadConf(conf: Config) {
    HDMContext.setDefaultConf(conf)
  }

  /**
   * server main startup
    *
    * @param args {
   *              case "-m" | "-master" => is this a Master
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
      case "-h" | "-host" => host = param._2
      case "-p" | "-port" => port = param._2.toInt
      case "-b" | "-bport" => blockServerPort = param._2.toInt
      case "-m" | "-master" => isMaster = param._2.toBoolean
      case "-P" | "-parent" => parentPath = if (!param._2.startsWith("akka.tcp://")) s"akka.tcp://masterSys@${param._2}/user/smsMaster" else param._2
      case "-M" | "-mode" => mode = param._2
      case "-n" | "-nodeType" => nodeType = param._2
      case "-s" | "-slots" => slots = param._2.toInt
      case "-mem" | "-memory" => mem = param._2
      case "-c" | "-conf" => try {
        defaultConf = ConfigFactory.load(param._2)
        loadConf(defaultConf)
      }
      case "-f" | "-file" => try {
        defaultConf = ConfigFactory.parseFile(new File(param._2))
        loadConf(defaultConf)
      }
      case other:String => // do nothing
    })

    val hDMContext = new HDMServerContext(defaultConf)
//    val hDMContext = HDMServerContext.defaultContext
    val engine = new HDMEngine(hDMContext)

    if (isMaster){
      log.info(s"starting master at $host:$port, with mode: $mode, " +
        s"CPU Parallel factor: ${hDMContext.PLANER_PARALLEL_CPU_FACTOR}, " +
        s"Network parallel factor: ${hDMContext.PLANER_PARALLEL_NETWORK_FACTOR}")
      nodeType match {
        case "app" => engine.startAsMaster(host= host, port = port, conf = defaultConf, mode = mode)//port, defaultConf
        case "cluster" => engine.startAsClusterMaster(host= host, port = port, conf = defaultConf, mode = mode)
      }
    } else {
      nodeType match {
        case "app" => engine.startAsSlave(parentPath, host, port, blockServerPort, defaultConf, slots)
        case "cluster" => engine.startAsClusterSlave(parentPath, host, port, blockServerPort, defaultConf, slots, mem)
      }
    }//parentPath, port, defaultConf
    log.info(s"HDM [$nodeType] node started  as ${if (isMaster) "master" else "slave"} at: $host:$port .")

  }


}
