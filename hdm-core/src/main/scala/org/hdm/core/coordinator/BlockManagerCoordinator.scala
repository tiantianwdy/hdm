package org.hdm.core.coordinator

import akka.actor.ActorPath
import akka.pattern._

import org.hdm.akka.actors.worker.WorkActor
import org.hdm.core.executor.{AppContext, HDMContext}
import org.hdm.core.io.{HDMIOManager, Path}
import org.hdm.core.storage.{Computed, HDMBlockManager}
import org.hdm.core.message._
import org.hdm.core.model.{ParHDM, DDM}

import scala.collection.JavaConversions._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

/**
 * Created by Tiantian on 2014/12/18.
 */

class BlockManagerLeader extends WorkActor {

  val blockManager = HDMBlockManager()

  val ioManager = HDMIOManager()

  val hDMContext = HDMContext.defaultHDMContext

  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  override def process: PartialFunction[Any, Unit] = {

    case AddRefMsg(refs, broadcast) =>
      val senderPath = sender.path
      val nRefs = refs.map { r =>
        r match {
        case ddm: DDM[_,_] =>
          if(ddm.location.protocol == Path.AKKA){
            val fullPath = ActorPath.fromString(ddm.location.toString).toStringWithAddress(senderPath.address)
            ddm.copy(location = Path(fullPath))
          } else ddm
        case x => r
        }
      }
      blockManager.addAllRef(nRefs)
      if(broadcast){
        for (follower <- followerMap.toSeq){
          if(follower._1 != senderPath.toString) context.actorSelection(follower._1) ! SyncRefMsg(refs)
        }
      }

      log.debug(s"Block references have has been added: [${nRefs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.removeRef(id)
      val senderPath = sender.path
      for (follower <- followerMap.toSeq){
        if(follower._1 != senderPath.toString) context.actorSelection(follower._1) ! RemoveRefMsg(id)
      }
      log.debug(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl, declare) =>
      blockManager.add(bl.id, bl)
      log.debug(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMsg(id) =>
      blockManager.removeBlock(id)
      val senderPath = sender.path
      val ref = blockManager.getRef(id)
      if (ref != null && ref.blocks != null) {
        val locations = ref.blocks
        for (location <- locations.toSeq) {
          val path = Path(location)
          if (path.parent != senderPath.toString
            && path.parent != hDMContext.localBlockPath) {
            log.info(s"send remove msg to: [${path.parent}] ")
            context.actorSelection(path.parent) ! RemoveBlockMsg(path.name)
          }
        }
        log.debug(s"A Block data has has been removed: [${id}] ")
      } else {
        log.warning(s"Cannot find reference of block: [${id}] ")
      }

    case QueryBlockMsg(ids, location) =>
      ids.foreach { id =>
        val bl = blockManager.getBlock(id) //find from cache
        if (bl != null)
          sender ! BlockData(id, bl)
        else context.actorSelection(location).tell(QueryBlockMsg(Seq(id), location), self) //find from remote
      }

    case BlockData(id, bl) =>
      blockManager.add(id, bl)
      ioManager.blockReceived(id,sender().path.toString, bl)
      log.debug(s"A Block data has has been received: [${id}] ")

    case CheckStateMsg(id) =>
      if (blockManager.getRef(id) != null) {
        val reply = BlockStateMsg(id, blockManager.getRef(id).state)
        sender ! reply
      }
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender.path.toString
      if (!followerMap.containsKey(senderPath))
        followerMap.put(senderPath, new AtomicInteger(state))
      log.info(s"A block manager has joined from [${senderPath}}] ")

    case LeaveMsg(senderPath) =>
      followerMap.remove(senderPath)
      log.info(s"A block manager has left from [${senderPath}}] ")

    case msg => log.warning(s"Unhandled msg: $msg")
  }
}

class BlockManagerFollower(val leaderPath: String) extends WorkActor {

  val hDMContext = HDMContext.defaultHDMContext

  val blockManager = HDMBlockManager()


  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, 1)
    1
  }


  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(Seq(self.path.toString))
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {

    case AddRefMsg(refs, broadcast) =>
      blockManager.addAllRef(refs)
      if (broadcast && sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(AddRefMsg(refs), self)
      log.debug(s"Block references have has been added: [${refs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case SyncRefMsg(refs) =>
      blockManager.addAllRef(refs)
      log.debug(s"Block references have has been synchonized: [${refs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.removeRef(id)
      if (sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(RemoveRefMsg(id), self)
      log.debug(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl, declare) =>
      blockManager.add(bl.id, bl)
      if (declare && sender.path.toString != leaderPath) {
        val id = HDMContext.newLocalId()
        val ddm = new DDM(id = id,
          state = Computed,
          location = Path(hDMContext.localBlockPath + "/" + id),
          blocks = mutable.Buffer(hDMContext.localBlockPath + "/" + id),
          appContext = AppContext())
        context.actorSelection(leaderPath).tell(AddRefMsg(Seq(ddm)), self)
      }
      log.debug(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMsg(id) =>
      blockManager.removeBlock(id)
      if (sender.path.toString != leaderPath)
        println(s"receve a remove block msg from ${sender.path.toString}, leader path is ${leaderPath}")
//        context.actorSelection(leaderPath).tell(RemoveBlockMsg(id), self)
      log.debug(s"A Block data has has been removed: [${id}] ")

    case QueryBlockMsg(ids, location) =>
      ids.foreach{id =>
        val bl = blockManager.getBlock(id) //find from cache
        if (bl != null)
          sender ! BlockData(id, bl)
        else context.actorSelection(location).tell(QueryBlockMsg(Seq(id), location), self) //find from remote
      }
    case BlockData(id, bl) =>
//      blockManager.add(id, bl)
      HDMIOManager().blockReceived(id,sender().path.toString, bl)
      log.debug(s"A Block data has has been received: [${id}] ")

    case CheckStateMsg(id) =>
      if (blockManager.getRef(id) != null) {
        val reply = BlockStateMsg(id, blockManager.getRef(id).state)
        sender ! reply
      } else context.actorSelection(leaderPath).tell(CheckStateMsg(id), sender)


    case msg => log.warning(s"Unhandled msg: $msg")
  }
}
