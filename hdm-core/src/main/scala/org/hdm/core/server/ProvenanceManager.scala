package org.hdm.core.server

import java.util.concurrent.{CopyOnWriteArrayList, ConcurrentHashMap}

import com.google.common.collect.Maps
import org.hdm.core.planing.{JobStageInfo, JobStage}
import org.hdm.core.server.provenance.{ExecutionDAG, ExecutionTrace, ApplicationTrace}

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

  def getAllAppIDs():Seq[String]

  def getAppInstances(appName:String, version:String):Seq[String]

  def getExecTrace(id:String):ExecutionTrace

  def getInstanceTraces(instanceId:String):Seq[ExecutionTrace]
  
  def getInstanceDAG(instanceId:String):ExecutionDAG

  def addJobStages(appID:String, stages:Seq[JobStage])

  def getJobStages(appID:String): Seq[JobStageInfo]

  def getAllApplicationIDs:Seq[String]

  def addExeStage(jobId:String, instanceId:String)

  def getInstanceIdStage(jobID:String):String
}

class ProvenanceManagerImpl extends ProvenanceManager {
  
  import scala.collection.JavaConversions._ 

  /**
   * appId -> version -> ApplicationTrace
   */
  private val appTraceMap = new ConcurrentHashMap[String, mutable.Map[String, ApplicationTrace]]()

  /**
   * taskId -> executionTrace
   */
  private val exeTraceMap = new ConcurrentHashMap[String, ExecutionTrace]()

  /**
   * indexes : instanceId -> Seq of executionTrace id
   */
  private val instanceHistory = new ConcurrentHashMap[String, mutable.Buffer[String]]

  /**
   * indexes : ${appName}#{$version} -> Seq of instanceId
   */
  private val appInstancesMap = new ConcurrentHashMap[String, mutable.Buffer[String]]

  /**
    * indexes for multi-cluster applications: appId -> seq of job stages
    * appId = [appName] + [version] + [instanceId]
    */
  private val stageMap = new ConcurrentHashMap[String, Seq[JobStageInfo]]()

  /**
    * stageId -> instanceId
    */
  private val stageInstanceMap = new ConcurrentHashMap[String, String]()

  override def addAppTrace(trace: ApplicationTrace): Unit = {
    appTraceMap.getOrElseUpdate(trace.name, 
      new ConcurrentHashMap[String, ApplicationTrace]) += trace.version -> trace
  }


  override def aggregateAppTrace(trace: ApplicationTrace): Unit = {
    if(containsApp(trace.name, trace.version)){
      val oldTrace = getAppTrace(trace.name, trace.version)
      val aggregatedDep = if(oldTrace.dependencies!= null) {
        (oldTrace.dependencies ++ trace.dependencies).toSet
      } else trace.dependencies
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
    if(appTraceMap.containsKey(appName)){
      appTraceMap.get(appName).containsKey(version)
    } else false
  }

  override def getAppTrace(appName: String, version: String): ApplicationTrace = {
    if(appTraceMap.containsKey(appName)){
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


  override def getAppInstances(appName: String, version: String): Seq[String] = {
    val appId = s"${appName}#${version}"
    appInstancesMap.get(appId)
  }


  override def getAllAppIDs(): Seq[String] = {
    appTraceMap.keys().toIndexedSeq
  }

  override def getInstanceTraces(instanceId: String) = {
    if(instanceHistory.containsKey(instanceId)){
      instanceHistory.get(instanceId).map(getExecTrace(_))
    } else {
      null
    }
  }

  override def getInstanceDAG(instanceId: String): ExecutionDAG = {
    getInstanceTraces(instanceId) match {
      case seq:Seq[ExecutionTrace] =>
        val links = seq.flatMap(exe => exe.inputPath.map(in => (in, exe.taskId)))
        ExecutionDAG(seq, links)
      case null => null
    }
  }

  override def addJobStages(appID: String, stages: Seq[JobStage]): Unit = {
    val simplifiedStages = stages.map(_.toJobStageInfo())
    stageMap.put(appID, simplifiedStages)
  }

  override def getJobStages(appID: String): Seq[JobStageInfo] = {
    stageMap.get(appID)
  }

  override def getAllApplicationIDs: Seq[String] = {
    stageMap.keySet().toIndexedSeq
  }

  override def addExeStage(jobId: String, instanceId: String): Unit = {
    stageInstanceMap.put(jobId, instanceId)
  }

  override def getInstanceIdStage(jobID: String): String = {
    stageInstanceMap.get(jobID)
  }
}


object ProvenanceManager {

  lazy val defaultProvenanceManager = new ProvenanceManagerImpl

  def apply() = defaultProvenanceManager
}
