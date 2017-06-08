package org.hdm.core.message

import java.nio.ByteBuffer

import org.hdm.core.storage.Block

/**
 * Created by tiantian on 4/06/15.
 */
case class NettyFetchRequest (msg:QueryBlockMsg, callback:FetchSuccessResponse => Unit) extends Serializable

case class FetchSuccessResponse(id:String, length:Int, data:ByteBuffer) extends Serializable
