/*
 * Copyright 2013-2019 BP&IT.
 */
package com.baidu.bpit.akka.messages

import java.util.Date
import com.baidu.bpit.akka.monitor.MonitorData
import com.baidu.bpit.akka.configuration.ActorConfig

/**
 * Master-slave通信消息族实现
 * wudongyao
 */
abstract class MasterSlaveProtocol(params: Any = null) extends BasicMessage(new Date())

/**
 * 初始化消息
 * @param config 初始化参数
 */
case class InitMsg(config: Any) extends MasterSlaveProtocol(params = config)

/**
 * 状态改变消息
 * @param state 改变的状态
 * @param cause 原因说明
 *
 * */
case class StateMsg(state: Int, cause:Any = "") extends MasterSlaveProtocol(params = state)

/**
 *  挂起消息
 * @param actorPath
 */
@deprecated("可以由StateMsg替代","0.0.1")
case class SuspendMsg(actorPath: String) extends MasterSlaveProtocol

/**
 * 更新消息
 * @param actorPath 被更新的actor路径
 * @param confName 需要更新的参数名
 * @param config 更新参数
 * @param discardOld 是否废弃原来的参数
 * @param propagate 是否传播这次更新
 */
case class UpdateMsg(actorPath: String, confName:String, config: Any, discardOld: Boolean = false, propagate : Boolean = true) extends MasterSlaveProtocol(params = config)

/**
 * 添加消息，用于创建Actor
 * @param name 名字
 * @param path 部署路径
 * @param clazz actor实现类
 * @param params 初始化参数
 */
case class AddMsg(name: String = "", path: String = "", clazz: String, params: Any) extends MasterSlaveProtocol(params = params)

/**
 *  停止消息
 */
case class StopMsg(parent:String= "", name:String) extends MasterSlaveProtocol

/**
 * 重启消息
 */
case object RestartMsg extends MasterSlaveProtocol

/**
 *  采集数据消息
 */
case object CollectMsg extends MasterSlaveProtocol

/**
 * 申请加入的消息
 * @param masterPath
 * @param slavePath
 */
case class JoinMsg(masterPath: String = null, slavePath: String = null) extends MasterSlaveProtocol

/**
 * 离开消息
 * @param masterPath
 * @param slavePath
 */
case class LeftMsg(masterPath: String = null, slavePath: String = null) extends MasterSlaveProtocol

/**
 * 心跳消息
 * @param slavePath
 * @param masterPath
 * @param monitorData
 */
case class HeartbeatMsg(slavePath: String = null, masterPath: String = null, monitorData: java.util.List[MonitorData] = null) extends MasterSlaveProtocol


case class HeartbeatMsgResp(state: Int) extends MasterSlaveProtocol


/**
 * 路由协议消息族实现
 */
abstract sealed class RoutingProtocol extends BasicMessage(new Date())

case class RoutingSyncMsg(confList: List[ActorConfig], discardOld: Boolean = false) extends RoutingProtocol

case class RoutingQueryMsg(path: String = "", flag: Int = 0) extends RoutingProtocol

case class RoutingAddMsg(conf: ActorConfig) extends RoutingProtocol

case class RoutingUpdateMsg(actorPath: String, conf: ActorConfig) extends RoutingProtocol

/**
 *
 * @param path
 * @param flag
 */
case class RoutingRemove(path: String = "", flag: Int = 0) extends RoutingProtocol


  