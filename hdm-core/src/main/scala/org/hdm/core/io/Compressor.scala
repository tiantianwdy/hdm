package org.hdm.core.io

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import com.typesafe.config.Config
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}

import scala.util.Try

/**
  * Created by tiantian on 11/06/15.
  */
trait CompressionCodec {

  def compress(bytes: Array[Byte]): Array[Byte]

  def compress(buf: ByteBuffer, length: Int): ByteBuffer

  def uncompress(bytes: Array[Byte]): Array[Byte]

  def uncompress(buf: ByteBuffer, length: Int): ByteBuffer

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}

class SnappyCompressionCodec(conf: Config) extends CompressionCodec with Serializable {

  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException
  }

  val blockSize = Try(conf.getInt("hdm.io.compression.snappy.block.size")) getOrElse (32768)


  override def compress(bytes: Array[Byte]): Array[Byte] = {
    Snappy.compress(bytes)
  }

  override def compress(buf: ByteBuffer, length: Int): ByteBuffer = {
    val compressed = ByteBuffer.allocate(length)
    Snappy.compress(buf, compressed)
    compressed
  }

  override def uncompress(bytes: Array[Byte]): Array[Byte] = {
    Snappy.uncompress(bytes)
  }


  override def uncompress(buf: ByteBuffer, length: Int): ByteBuffer = {
    val uncompressed = ByteBuffer.allocate(length)
    Snappy.uncompress(buf, uncompressed)
    uncompressed
  }

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}