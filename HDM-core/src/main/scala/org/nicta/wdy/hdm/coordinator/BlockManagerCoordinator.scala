package org.nicta.wdy.hdm.coordinator

import akka.pattern._

import com.baidu.bpit.akka.actors.worker.WorkActor
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.message._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap
import org.nicta.wdy.hdm.model.DDM

/**
 * Created by Tiantian on 2014/12/18.
 */

class BlockManagerLeader extends WorkActor {

  val blockManager = HDMBlockManager()

  val followerMap:java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  override def process: PartialFunction[Any, Unit] = {

    case AddRefMsg(refs) =>
      blockManager.addAllRef(refs)
      log.info(s"Block references have has been added: [${refs.map(_.id).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.remove(id)
      log.info(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl) =>
      blockManager.add(bl.id, bl)
      log.info(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMSg(id) =>
      blockManager.remove(id)
      log.info(s"A Block data has has been removed: [${id}] ")

    case QueryBlockMsg(id) =>
      val bl = blockManager.getBlock(id)
      if(bl != null)
        sender ! BlockData(bl)

    case CheckStateMsg(id) =>
      if(blockManager.getRef(id) != null){
        val reply = BlockStateMsg(id, blockManager.getRef(id).state)
        sender ! reply
      }
    // coordinating msg
    case JoinMsg(senderPath, state) =>
      if(!followerMap.containsKey(senderPath))
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
      if(sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(AddRefMsg(refs), sender)
      log.info(s"Block references have has been added: [${refs.map(_.id).mkString(",")}] ")

    case RemoveRefMsg(id) =>
      blockManager.remove(id)
      if(sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(RemoveRefMsg(id), sender)
      log.info(s"A Block reference has has been removed: [${id}] ")

    case AddBlockMsg(bl) =>
      blockManager.add(bl.id, bl)
      if(sender.path.toString != leaderPath){
        val id = HDMContext.newLocalId()
        val ddm = new DDM(id= id,
          state = Computed,
          location = Path(Path.HDM, HDMContext.localContextPath + id))
        context.actorSelection(leaderPath).tell(AddRefMsg(Seq(ddm)), sender)
      }
      log.info(s"A Block data has has been added: [${bl.id}] ")

    case RemoveBlockMSg(id) =>
      blockManager.remove(id)
      if(sender.path.toString != leaderPath)
        context.actorSelection(leaderPath).tell(RemoveBlockMSg(id), sender)
      log.info(s"A Block data has has been removed: [${id}] ")

    case QueryBlockMsg(id) =>
      val bl = blockManager.getBlock(id)
      if(bl != null)
        sender ! BlockData(bl)
      else context.actorSelection(leaderPath).tell(QueryBlockMsg(id), sender)

    case CheckStateMsg(id) =>
      if(blockManager.getRef(id) != null){
        val reply = BlockStateMsg(id, blockManager.getRef(id).state)
        sender ! reply
      } else context.actorSelection(leaderPath).tell(CheckStateMsg(id), sender)


    case msg => log.warning(s"Unhandled msg: $msg")
  }
}
