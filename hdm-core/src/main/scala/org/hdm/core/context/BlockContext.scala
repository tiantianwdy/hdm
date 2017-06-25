package org.hdm.core.context

import org.hdm.akka.server.SmsSystem
import org.hdm.core.io.netty.NettyConnectionManager

/**
 * Created by tiantian on 28/04/16.
 */
case class BlockContext (blockManagerPath:String, protocol:String, serverPort:Int) extends Serializable {

  def localBlockPath = {
    protocol match {
      case "akka" => SmsSystem.physicalRootPath + "/" + HDMContext.BLOCK_MANAGER_NAME
      case "netty" => s"netty://${NettyConnectionManager.localHost}:${serverPort}"
    }
  }
}
