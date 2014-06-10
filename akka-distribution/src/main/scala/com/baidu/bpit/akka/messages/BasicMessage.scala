package com.baidu.bpit.akka.messages

import java.util.Date

/**
 * 所有消息的基类主要继承了Serializable接口，提供时间戳字段
 * wudongyao
 */
abstract class BasicMessage( var lastModifiedTimeStamp:Date = new Date()) extends Serializable {
  

}

