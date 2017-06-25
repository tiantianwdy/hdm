package org.hdm.core.serializer

import java.nio.ByteBuffer

import org.hdm.core.context.HDMContext
import org.hdm.core.server.HDMServerContext

/**
 * Created by tiantian on 8/03/16.
 */
class DynamicAkkaSerializer extends akka.serialization.Serializer{

  val serializer = HDMContext.DEFAULT_SERIALIZER
  
  override def identifier: Int = 1

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    serializer.deserialize(ByteBuffer.wrap(bytes))
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    serializer.serialize(o).array()
  }

}
