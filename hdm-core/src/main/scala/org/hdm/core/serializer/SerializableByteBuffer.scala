package org.hdm.core.serializer

import java.io.{IOException, ObjectOutputStream, EOFException, ObjectInputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels



/**
 * Created by tiantian on 9/03/16.
 */
class SerializableByteBuffer (var buffer: ByteBuffer) extends Serializable {
  
  def value = buffer

  private def readObject(in: ObjectInputStream): Unit = try {
    val length = in.readInt()
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  private def writeObject(out: ObjectOutputStream): Unit = try {
    out.writeInt(buffer.limit())
    if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind() // Allow us to write it again later
  }
}
