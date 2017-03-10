package org.hdm.core.message

import org.hdm.core.model.{HDMPoJo, HDM}
import org.hdm.core.planing.{JobStageInfo, JobStage}
import org.hdm.core.server.provenance.{ApplicationTrace, ExecutionTrace}

/**
 * Created by tiantian on 7/04/16.
 */
trait QueryMsg extends Serializable


case class ApplicationsQuery() extends QueryMsg

case class ApplicationsResp(results:Seq[(String, Seq[String])]) extends QueryMsg

case class ApplicationInsQuery(appName:String, version:String) extends QueryMsg

case class ApplicationInsResp(appName:String, version:String, results:Seq[String]) extends QueryMsg

case class ExecutionTraceQuery(execId:String) extends QueryMsg

case class ExecutionTraceResp(execId:String, results:Seq[ExecutionTrace])

case class LogicalFLowQuery(execId:String, opt:Boolean) extends QueryMsg

case class LogicalFLowQueryByStage(jobId:String, opt:Boolean) extends QueryMsg

case class LogicalFLowResp(execId:String, results:Seq[HDMPoJo])extends QueryMsg

case class PhysicalFlow(execId:String) extends QueryMsg

case class PhysicalFlowResp(execId:String, results:Seq[HDMPoJo])extends QueryMsg

case class AllSlavesQuery(parent:String)extends QueryMsg

case class AllSLavesResp(results:Seq[NodeInfo])extends QueryMsg

case class AllAppVersionsQuery() extends QueryMsg

case class AllAppVersionsResp(results:Seq[(String, Seq[String])]) extends QueryMsg

case class DependencyTraceQuery(appName:String, version:String) extends QueryMsg

case class DependencyTraceResp(appName:String, version:String, results:Seq[(String, ApplicationTrace)]) extends QueryMsg

case class DescendantQuery(parent:String)extends QueryMsg

case class JobStagesQuery(appName:String, version:String) extends QueryMsg

case class JobStageResp(appName:String, version:String, results:Seq[JobStageInfo]) extends QueryMsg

case class AllApplicationsQuery() extends QueryMsg

case class AllApplicationsResp(results:Seq[String]) extends QueryMsg

case class NodeInfo(id:String, typ:String, parent:String, address:String,  joinTime:Long,  slots:Int, state:String) extends  Serializable