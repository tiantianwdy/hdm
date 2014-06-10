package com.baidu.bpit.akka.monitor

import scala.beans.BeanProperty

/**
 * @author wudongyao
 * @date 2013-7-2 
 * @version
 *
 */
case class MonitorData(
                        @BeanProperty monitorName: String,
                        @BeanProperty prop: String,
                        @BeanProperty key: String,
                        @BeanProperty value: String,
                        @BeanProperty source: String,
                        @BeanProperty time: Long = System.currentTimeMillis()) extends Serializable {

}