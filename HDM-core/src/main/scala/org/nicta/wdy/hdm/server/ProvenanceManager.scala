package org.nicta.wdy.hdm.server

import java.util.concurrent.{CopyOnWriteArrayList, ConcurrentHashMap}

import com.google.common.collect.Maps
import org.nicta.wdy.hdm.server.provenance.{ExecutionDAG, ExecutionTrace, ApplicationTrace}

import scala.collection.mutable

/**
 * Created by tiantian on 3/03/16.
 */
trait ProvenanceManager {

  def addAppTrace(trace: ApplicationTrace): Unit

  def addExecTrace(exeTrace: ExecutionTrace): Unit

  def updateExecTrace(exeTrace: ExecutionTrace): Unit

  def containsApp(appName:String, version:String): Boolean

  def aggregateAppTrace(trace: ApplicationTrace): Unit

  def getAppTrace(appName:String, version:String): ApplicationTrace

  def getAppTraces(appName:String):Seq[(String, ApplicationTrace)]

  def getExecTrace(id:String):ExecutionTrace

  def getInstanceTraces(instanceId:String):Seq[ExecutionTrace]
  
  def getInstanceDAG(instanceId:String):ExecutionDAG

}

class ProvenanceManagerImpl extends ProvenanceManager {
  
  import scala.collection.JavaConversions._ 

  /**
   * appId -> version -> ApplicationTrace
   */
  private val appTraceMap = new ConcurrentHashMap[String, mutable.Map[String, ApplicationTrace]]()
  
  private val exeTraceMap = new ConcurrentHashMap[String, ExecutionTrace]()

  /**
   * indexes : instanceId -> Seq of executionTrace id
   */
  private val instanceHistory = new ConcurrentHashMap[String, mutable.Buffer[String]]

  /**
   * indexes : ${pipeName}#{$version} -> Seq of instanceId
   */
  private val appInstancesMap = new ConcurrentHashMap[String, mutable.Buffer[String]]

  override def addAppTrace(trace: ApplicationTrace): Unit = {
    appTraceMap.getOrElseUpdate(trace.name, 
      new ConcurrentHashMap[String, ApplicationTrace]) += trace.version -> trace
  }


  override def aggregateAppTrace(trace: ApplicationTrace): Unit = {
    if(containsApp(trace.name, trace.version)){
      val oldTrace = getAppTrace(trace.name, trace.version)
      val aggregatedDep = (oldTrace.dependencies ++ trace.dependencies).toSet
      addAppTrace(trace.copy(dependencies = aggregatedDep.toSeq))
    } else addAppTrace(trace)
  }

  override def getExecTrace(id: String): ExecutionTrace = {
    exeTraceMap.get(id)
  }

  override def updateExecTrace(exeTrace: ExecutionTrace): Unit = {
    exeTraceMap.put(exeTrace.taskId, exeTrace)
  }


  override def containsApp(appName: String, version: String): Boolean = {
    if(appTraceMap.contains(appName)){
      appTraceMap.contains(version)
    } else false
  }

  override def getAppTrace(appName: String, version: String): ApplicationTrace = {
    if(appTraceMap.contains(appName)){
      appTraceMap.get(appName).getOrElse(version, null)
    } else null
  }

  override def addExecTrace(exeTrace: ExecutionTrace): Unit = {
    val appId = s"${exeTrace.appName}#${exeTrace.version}"
    appInstancesMap.getOrElseUpdate(appId, new CopyOnWriteArrayList[String]) += exeTrace.instanceID
    instanceHistory.getOrElseUpdate(exeTrace.instanceID, new CopyOnWriteArrayList[String]) += exeTrace.taskId
    exeTraceMap.put(exeTrace.taskId, exeTrace)
  }

  override def getAppTraces(appName: String): Seq[(String, ApplicationTrace)] = {
    appTraceMap.getOrElse(appName, Seq.empty[(String, ApplicationTrace)]).toSeq
  }

  override def getInstanceTraces(instanceId: String) = {
    if(instanceHistory.contains(instanceId)){
      instanceHistory.get(instanceId).map(getExecTrace(_))
    } else null
  }

  override def getInstanceDAG(instanceId: String): ExecutionDAG = {
    getInstanceTraces(instanceId) match {
      case seq:Seq[ExecutionTrace] =>
        val links = seq.flatMap(exe => exe.inputPath.map(in => (in, exe.taskId)))
        ExecutionDAG(seq, links)
      case null => null
    }
  }
}


object ProvenanceManager {

  lazy val defaultProvenanceManager = new ProvenanceManagerImpl

  def apply() = defaultProvenanceManager
}
