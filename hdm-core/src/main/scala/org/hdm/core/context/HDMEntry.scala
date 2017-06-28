package org.hdm.core.context

import org.hdm.akka.server.SmsSystem
import org.hdm.core.message._
import org.hdm.core.model.{ParHDM, HDM}
import org.hdm.core.storage.Block

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by tiantian on 26/06/17.
  */
trait HDMEntry {

  val hDMContext:HDMContext

  val executionContext: ExecutionContext

  def init(leader: String, slots: Int, port: Int): Unit

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]]

  def clean(appId: String) = {
    //todo clean all the resources used by this application
    val masterURL = hDMContext.leaderPath.get() + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME
    SmsSystem.askAsync(masterURL, ApplicationShutdown(appId))
  }

  def shutdown(appContext: AppContext = AppContext.defaultAppContext) {
    clean(appContext.appName + "#" + appContext.version)
    SmsSystem.shutDown()
  }


  def declareHdm(hdms: Seq[ParHDM[_, _]], declare: Boolean = true) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddRefMsg(hdms, declare))
  }

  def addBlock(block: Block[_], declare: Boolean) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddBlockMsg(block, declare))
  }

  def queryBlock(id: String, location: String) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, QueryBlockMsg(Seq(id), location))
  }

  def removeBlock(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveBlockMsg(id))
  }

  def removeRef(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveRefMsg(id))
  }

  def addShutdownHook(appContext: AppContext): Unit ={
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        shutdown(appContext)
        Thread.sleep(1000)
        SmsSystem.shutDown()
      }
    })
  }
}
