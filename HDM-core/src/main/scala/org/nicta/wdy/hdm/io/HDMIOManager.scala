package org.nicta.wdy.hdm.io

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Block, BlockRef}
import scala.concurrent.{Promise, Future}
import org.hdm.akka.server.SmsSystem
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by Tiantian on 2014/12/1.
 */
trait HDMIOManager {

  def askBlock(id:String, path:String):Future[Block[_]]

  def sendBlock(id:String, path:String, bl: Block[_]):Unit

  def loadBlock[T](id:String, path:String):Future[Block[T]]

  protected def loadFromRemote[T](id:String, path:String):Block[T]

  def queryReceived(id:String, fromPath:String):Unit

  def blockReceived (id:String, fromPath:String, srcBlock: Block[_]):Unit

}

object HDMIOManager {

  lazy val defaultIOManager = new AkkaIOManager(HDMContext.defaultHDMContext) // todo change to load config

  def apply():HDMIOManager = {
    defaultIOManager
  }

}

/**
 *
 */
class AkkaIOManager(hDMContext: HDMContext) extends HDMIOManager {

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  val blockManager = HDMBlockManager()

  override def loadBlock[T](id: String, path: String): Future[Block[T]] = ??? // todo submit a file load task to local execution scheduler

  override def sendBlock(id: String, path: String, bl: Block[_]): Unit = {
    SmsSystem.forwardMsg(path, bl)
  }

  override def askBlock(id: String, path: String): Future[Block[_]] = {
    val promise = Promise[Block[_]]
    promiseMap.put(id, promise)
    hDMContext.queryBlock(id, path)
    promise.future
  }

  override def blockReceived(id: String, fromPath: String, srcBlock: Block[_]): Unit = {
//    blockManager.add(id, srcBlock)
    val promise = promiseMap.get(id).asInstanceOf[Promise[Block[_]]]
    if(promise ne null)
      promise.success(srcBlock)
  }


  override def queryReceived(id: String, fromPath: String): Unit = {
    val block = HDMBlockManager().getBlock(id)
    sendBlock(id, fromPath, block)
  }

  override protected def loadFromRemote[T](id: String, path: String): Block[T] = ???
}

/**
 *
 */
abstract class NettyIOManager extends HDMIOManager {


}
