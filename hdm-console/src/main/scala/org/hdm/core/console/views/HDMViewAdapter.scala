package org.hdm.core.console.views

import java.util.Date

import org.hdm.akka.configuration.ActorConfig
import org.hdm.core.console.models._
import org.hdm.core.io.Path
import org.hdm.core.messages._
import org.hdm.core.model.HDMInfo
import org.hdm.core.server.provenance.ExecutionTrace

import scala.collection.mutable

/**
 * Created by tiantian on 8/04/16.
 */
object HDMViewAdapter {

  import scala.collection.JavaConversions._

  def validId(str: String) = {
    str.replaceAll("_p(\\d+)", "")
  }

  def allApplicationsRespToTreeVO(resp: AllApplicationsResp): TreeVO = {
    val data= resp.results
    val root = new TreeVO("Applications")
    data.foreach{ app =>
      val child = new TreeVO(app)
      root.addChild(child)
    }
    root
  }

  def applicationsRespToTreeVO(resp: ApplicationsResp): TreeVO = {
    val data = resp.results
    mapToTreeVO("All Jobs", data)
  }

  def allVersionsRespToTreeVO(resp: AllAppVersionsResp):TreeVO = {
    val data = resp.results
    mapToTreeVO("Applications", data)
  }

  def dependencyTraceToTreeVO(resp:DependencyTraceResp):TreeVO = {
    val data = resp.results
    val root = new TreeVO(resp.appName)
    data.foreach {
      tup => //(version, dependencyTrace)
        val child = new TreeVO(tup._1)
        root.addChild(child)
        child.addChild(new TreeVO("Version: " + tup._2.version))
        child.addChild(new TreeVO("Author: " + tup._2.author))
        child.addChild(new TreeVO("Create Time: " + new Date(tup._2.createTime).toLocaleString))
        if(tup._2.dependencies ne null){
          val depNode = new TreeVO("Dependencies")
          child.addChild(depNode)
          val dep = tup._2.dependencies
          dep.foreach(url => depNode.addChild(new TreeVO(url)))
        }
    }
    root
  }

  def slaveListToTreeVO(master:String, data:Seq[ActorConfig]): TreeVO ={
    val rootName = Path(master).address
    val root = new TreeVO(rootName, 1)
    data.foreach {
      actor =>
        val addr = Path(actor.actorPath).address
        val child = new TreeVO(addr, actor.deploy.state)
        root.addChild(child)
    }
    root
  }

  def slaveMonitorVO(data:Any):Array[Array[Any]] = {
    data match {
      case arr: Array[Array[Any]] => arr
      case _ => Array.empty[Array[Any]]
    }
  }

  def HDMPojoSeqToGraph(data: Seq[HDMInfo]): DagGraph = {
      val graph = new DagGraph()
      val nodes = mutable.Buffer.empty[HDMNode]
      val nodeIdxes = mutable.HashMap.empty[String, HDMNode]
      val links = mutable.Buffer.empty[DagLink]

      data.foreach { hdm =>
        val input = if(hdm.children != null) hdm.children.toArray else Array.empty[String]
        val output = if(hdm.blocks != null) hdm.blocks.toArray else null
        val node = new HDMNode (hdm.id, hdm.name, "version", hdm.func, hdm.hdmType , hdm.locationStr,
          hdm.dependency, hdm.parallelism.toString, hdm.partitioner, input, output, hdm.isCache,
          0L, 0L, hdm.state, statusToGroup(hdm.state))
        nodeIdxes += (node.getId -> node)
        nodes += node
      }

      data.foreach { hdm => if (hdm.children ne null) {
        hdm.children.foreach { child =>
          val id = validId(child)
          if (nodeIdxes.contains(id)) {
            val parent = nodeIdxes(id)
            links += (new DagLink(parent.getId, hdm.id, ""))
          }
        }
      }
      }

      graph.setLinks(links)
      graph.setNodes(nodes)
      graph
  }

  def HDMPojoSeqToGraphAggregated(data: Seq[HDMInfo]): DagGraph = {
    // todo aggregate same input (DDM) for each HDM into one node to avoid performance problem and filter out DDMs group the ddms based on their output
    val graph = new DagGraph()
    val nodes = mutable.Buffer.empty[HDMNode]
    val links = mutable.Buffer.empty[DagLink]

    val nodeIdxes = mutable.HashMap.empty[String, HDMNode]
    val ddmGroups = mutable.HashMap.empty[String, mutable.Buffer[String]]


    data.foreach { hdm =>
      val input = if(hdm.children != null) hdm.children.toArray else Array.empty[String]
      val output = if(hdm.blocks != null) hdm.blocks.toArray else null
      val node = new HDMNode (hdm.id, hdm.name, "version", hdm.func, hdm.hdmType , hdm.locationStr,
        hdm.dependency, hdm.parallelism.toString, hdm.partitioner, input, output, hdm.isCache,
        0L, 0L, hdm.state, statusToGroup(hdm.state))
      nodeIdxes += (node.getId -> node)
      if(node.getType != "DDM"){
        nodes += node
      }
    }

    data.foreach { hdm => if (hdm.children ne null) {
      hdm.children.foreach { child =>
        val id = validId(child)
        if (nodeIdxes.contains(id)) {
          val parent = nodeIdxes(id)
          if (parent.getType == "DDM") {
            val groupId = s"Grouped_${hdm.id}"
            val buff = if (ddmGroups.contains(groupId)) {
              ddmGroups.get(groupId).get
            } else {
              // add grouped DDM node when create the group first time
              parent.setId(groupId)
              nodes += parent
              mutable.Buffer.empty[String]
            }
            buff += parent.getId
            links += (new DagLink(groupId, hdm.id, ""))
          } else {
            links += (new DagLink(parent.getId, hdm.id, ""))
          }
        }
      }
    }
    }
    graph.setLinks(links)
    graph.setNodes(nodes)
    graph
  }


  def executionTraceToGraph(resp: ExecutionTraceResp): DagGraph = {
    val data = resp.results
    val graph = new DagGraph()
    val nodes = mutable.Buffer.empty[HDMNode]
    val nodeIdxes = mutable.HashMap.empty[String, HDMNode]
    val links = mutable.Buffer.empty[DagLink]

    data.foreach { exe =>
      val input = if(exe.inputPath != null) exe.inputPath.toArray else Array.empty[String]
      val output = if(exe.outputPath != null) exe.outputPath.toArray else null
      val node = new HDMNode(exe.taskId, exe.funcName, exe.version, exe.function, "task", exe.location,
        exe.dependency, "", exe.partitioner, input, output,
        if(exe.status.toLowerCase == "completed") true else false,
        exe.startTime, exe.endTime, exe.status, statusToGroup(exe.status))
      nodeIdxes += (node.getId -> node)
      nodes += node
    }
    data.foreach { exe => if (exe.inputPath != null && exe.inputPath.nonEmpty) {
      exe.inputPath.foreach { child =>
        val id = validId(child)
        if (nodeIdxes.contains(id)) {
          val parent = nodeIdxes(id)
          links += (new DagLink(parent.getId, exe.taskId, s"${exe.endTime - exe.startTime}"))
        }
      }
     }
    }
    graph.setLinks(links)
    graph.setNodes(nodes)
    graph
  }

  def StagesToGraph(resp: JobStageResp): DagGraph = {
    val data = resp.results
    val graph = new DagGraph()
    val nodes = mutable.Buffer.empty[StageNode]
    val nodeIdxes = mutable.HashMap.empty[String, StageNode]
    val links = mutable.Buffer.empty[DagLink]

    data.foreach { stage =>
      val node = new StageNode(stage.appId, stage.jobId, stage.parents, stage.context, stage.jobFunc, stage.parallelism, stage.isLocal, "waiting")
      nodeIdxes += (node.getId -> node)
      nodes += node
    }
    data.foreach { stage => if (stage.parents != null && stage.parents.nonEmpty) {
      stage.parents.foreach { child =>
        if (nodeIdxes.contains(child)) {
          val parent = nodeIdxes(child)
          links += (new DagLink(parent.getId, stage.jobId, s"${child}"))
        }
      }
    }
    }
    graph.setLinks(links)
    graph.setNodes(nodes)
    graph
  }

  def slaveClusterToGraphVO(data:Seq[NodeInfo]):DagGraph = {
    val graph = new DagGraph()
    val nodes = mutable.Buffer.empty[D3Node]
    val links = mutable.Buffer.empty[D3Link]
    val nodeIdxes = mutable.HashMap.empty[String, Int]
    nodeIdxes ++= data.zipWithIndex.map(tup => tup._1.id -> tup._2)
    data.foreach{
      n =>
        val idx = nodeIdxes(n.id)
        nodes += new D3Node(idx, n.address, n.typ, n.parent, n.id, n.state, n.slots)
        if (n.parent != null && nodeIdxes.contains(n.parent)) {
          val pIdx = nodeIdxes(n.parent)
          links += new D3Link(idx, pIdx, "")
        }
    }
    graph.setLinks(links)
    graph.setNodes(nodes)
    graph
  }

  def executionTraceToLanes(resp: ExecutionTraceResp): TimeLanes = {
    val data = resp.results
    val taskWorkerMap = mutable.HashMap.empty[String, mutable.Buffer[ExecutionTrace]]
    val laneIdx = mutable.HashMap.empty[String, Int]
    var minRange = System.currentTimeMillis()
    var maxRange = 0L
    val idxParttern = "_b"
    if (data ne null) {
      data.foreach { exe =>
//        val coreIdx = if (!exe.taskId.containsSlice(idxParttern)) "0"
//        else {
//          val seq = exe.taskId.split(idxParttern)
//          if (seq.length > 0) seq(1) else "0"
//        }
        val coreIdx = exe.coreIdx
        if (exe.location ne null) {
          val workerId = Path(exe.location).address + "-" + coreIdx
          taskWorkerMap.getOrElseUpdate(workerId, mutable.Buffer.empty[ExecutionTrace]) += exe
        }
        if (exe.startTime < minRange) minRange = exe.startTime
        if (exe.endTime > maxRange) maxRange = exe.endTime
      }
    }

    val lanes = taskWorkerMap.keySet.toSeq.zipWithIndex.map { tup =>
      new Lane(tup._2, tup._1)
    }
    lanes.foreach(lane => laneIdx += lane.getLabel -> lane.getId)

    val items = taskWorkerMap.map { mapping =>
      val laneId = laneIdx.get(mapping._1) match {
        case Some(d) => d
        case None => 0
      }
      mapping._2.map { exe =>
        val time = if (exe.endTime < exe.startTime) System.currentTimeMillis() else exe.endTime
        new LaneItem(exe.taskId, laneId, exe.startTime, time, exe.status, exe.function)
      }
    }.flatten.toSeq
    if (maxRange <= 0) maxRange = System.currentTimeMillis()

    new TimeLanes(lanes, items, minRange, maxRange)
  }

  def mapToTreeVO(rootName:String, data:Seq[(String,Seq[String])]): TreeVO ={
    val root = new TreeVO(rootName)
    data.foreach {
      tup =>
        val child = new TreeVO(tup._1)
        root.addChild(child)
        tup._2.foreach(elem => child.addChild(new TreeVO(elem)))
    }
    root
  }

  def statusToGroup(state: String): Int = state.toLowerCase match {
    case "completed" | "computed" => 1
    case "running" => 2
    case "declared" => 0
    case other: String => 0
  }
}
