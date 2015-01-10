package org.nicta.wdy.hdm.coordinator

import akka.actor.ActorPath
import akka.pattern._

import com.baidu.bpit.akka.actors.worker.WorkActor
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.{HDMIOManager, Path}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.model.{HDM, DDM}

import scala.collection.JavaConversions._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by Tiantian on 2014/12/18.
 */

class BlockManagerLeader extends WorkActor {

  val blockManager = HDMBlockManager()

  val ioManager = HDMIOManager()

  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  override def process: PartialFunction[Any, Unit] = {

    case AddRefMsg(refs) =>
      val senderPath = sender.path
      val nRefs = refs.map { r =>
        r match {
        case ddm: DDM[_] =>
          if(ddm.location.protocol == Path.AKKA){
            val fullPath = ActorPath.fromString(ddm.location.toString).toStringWithAddress(senderPath.address)
            ddm.copy(location = Path(fullPath))
          } else ddm
        case x => r
        }
      }
      blockManager.addAllRef(refs)
      for (follower <- followerMap.toSeq){
        if(follower._1 != senderPath.toString) context.actorSelection(follower._1) ! SyncRefMsg(refs)
      }
      log.info(s"Block references have has been added: [${nRefs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.remove(id)
      log.info(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl) =>
      blockManager.add(bl.id, bl)
      log.info(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMSg(id) =>
      blockManager.remove(id)
      log.info(s"A Block data has has been removed: [${id}] ")

    case QueryBlockMsg(id, location) =>
      val bl = blockManager.getBlock(id) //find from cache
      if (bl != null)
        sender ! BlockData(id, bl)
      else context.actorSelection(location).tell(QueryBlockMsg(id, location), self) //find from remote

    case BlockData(id, bl) =>
      blockManager.add(id, bl)
      ioManager.blockReceived(id,sender().path.toString, bl)
      log.info(s"A Block data has has been received: [${id}] ")

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


  val blockManager = HDMBlockManager()


  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, 1)
    1
  }


  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(self.path.toString)
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {

    case AddRefMsg(refs) =>
      blockManager.addAllRef(refs)
      if (sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(AddRefMsg(refs), self)
      log.info(s"Block references have has been added: [${refs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case SyncRefMsg(refs) =>
      blockManager.addAllRef(refs)
      log.info(s"Block references have has been synchonized: [${refs.map(r => (r.id, r.location.toString)).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.remove(id)
      if (sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(RemoveRefMsg(id), self)
      log.info(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl) =>
      blockManager.add(bl.id, bl)
      if (sender.path.toString != leaderPath) {
        val id = HDMContext.newLocalId()
        val ddm = new DDM(id = id,
          state = Computed,
          location = Path(HDMContext.localBlockPath + "/" + id))
        context.actorSelection(leaderPath).tell(AddRefMsg(Seq(ddm)), self)
      }
      log.info(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMSg(id) =>
      blockManager.remove(id)
      if (sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(RemoveBlockMSg(id), self)
      log.info(s"A Block data has has been removed: [${id}] ")

    case QueryBlockMsg(id, location) =>
      val bl = blockManager.getBlock(id) //find from cache
      if (bl != null)
        sender ! BlockData(id, bl)
      else context.actorSelection(location).tell(QueryBlockMsg(id, location), self) //find from remote

    case BlockData(id, bl) =>
      blockManager.add(id, bl)
      HDMIOManager().blockReceived(id,sender().path.toString, bl)
      log.info(s"A Block data has has been received: [${id}] ")

    case CheckStateMsg(id) =>
      if (blockManager.getRef(id) != null) {
        val reply = BlockStateMsg(id, blockManager.getRef(id).state)
        sender ! reply
      } else context.actorSelection(leaderPath).tell(CheckStateMsg(id), sender)


    case msg => log.warning(s"Unhandled msg: $msg")
  }
}
