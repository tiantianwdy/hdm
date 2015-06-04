package org.nicta.wdy.hdm.message

import org.nicta.wdy.hdm.storage.Block

/**
 * Created by tiantian on 4/06/15.
 */
case class NettyCallbackRequest (msg:Any, callback:Block[_]=>Unit)
