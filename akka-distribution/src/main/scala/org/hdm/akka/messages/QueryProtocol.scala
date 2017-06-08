package org.hdm.akka.messages

import java.util.Date

/**
 *
 * @author wudongyao
 * @date 14-1-2,下午1:19
 * @version
 */
abstract  sealed  class QueryProtocol extends BasicMessage( new Date())

/**
 *
 * @param op
 *           要查询的内容 see: [[org.hdm.akka.actors.MasterQueryExtensionImpl#handleQueryMsg(org.hdm.akka.messages.QueryProtocol)]]
 * @param prop
 *             see: [[org.hdm.akka.extensions.CachedDataQueryService#handleQueryMsg(org.hdm.akka.messages.QueryProtocol)]]
 * @param key  as `prop`
 * @param duration 查询的时间范围，Long型
 * @param source  发送请求者
 * @param target  处理本次Query的Actor
 */
case class Query(op: String, prop: String, key: String, duration: Long = -1L, source: String = "", target: String = "") extends QueryProtocol

/**
 *
 * @param result
 * @param op
 * @param prop
 * @param key
 * @param source
 * @param target
 */
case class Reply(result:Any, op:String ="", prop: String = "", key: String = "", source:String = "", target:String = "") extends QueryProtocol


trait QueryExtension {

  def handleQueryMsg(msg: QueryProtocol): Option[Reply]

}
