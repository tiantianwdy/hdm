package org.hdm.core.message

/**
  * Created by tiantian on 10/05/17.
  */
trait ClusterMsg  extends Serializable

case class InitAppMaster(host:String, port:Int, mem:String, mode:String) extends ClusterMsg

case class InitExecutorMsg(appMaster:String, core:Int = -1, mem:String = "", port:Int = 12010, blockServerPort:Int = 9091) extends ClusterMsg

case class ShutdownMaster(graceful:Boolean, shutdownWorker: Boolean = true) extends ClusterMsg

case class ShutdownExecutor(id:String, graceful:Boolean) extends ClusterMsg

case object RestartExecutors extends ClusterMsg
