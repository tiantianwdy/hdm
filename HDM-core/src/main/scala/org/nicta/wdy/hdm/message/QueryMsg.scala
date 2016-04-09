package org.nicta.wdy.hdm.message

import org.nicta.wdy.hdm.model.{HDMPoJo, AbstractHDM}
import org.nicta.wdy.hdm.server.provenance.{ApplicationTrace, ExecutionTrace}

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

case class LogicalFLowResp(execId:String, results:Seq[HDMPoJo])extends QueryMsg

case class DependencyTraceQuery(appName:String, version:String) extends QueryMsg

case class DependencyTraceResp(appName:String, version:String, results:Seq[(String, ApplicationTrace)]) extends QueryMsg
