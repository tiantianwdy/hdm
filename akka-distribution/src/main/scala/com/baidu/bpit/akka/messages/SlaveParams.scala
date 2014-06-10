package com.baidu.bpit.akka.messages

import com.baidu.bpit.akka.configuration.ActorConfig

/**
 * slave节点所需的初始化参数消息类
 * @param params  slave 的初始化参数，数组型
 */
case class SlaveParams( params : List[ActorConfig]) extends Serializable{
  

}
