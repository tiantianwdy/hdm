package com.baidu.bpit.akka.extensions

import akka.actor.{ExtensionKey, Extension, ExtendedActorSystem}

/**
 * Created by tiantian on 6/01/15.
 */
class RemoteAddressExtensionImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}

object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtensionImpl]