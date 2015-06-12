package org.nicta.wdy.hdm.io

import java.io.{InputStream, OutputStream}

import com.typesafe.config.Config
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream, Snappy}

import scala.util.Try

/**
 * Created by tiantian on 11/06/15.
 */
trait CompressionCodec {

  def compress(bytes: Array[Byte]):Array[Byte]

  def uncompress(bytes: Array[Byte]):Array[Byte]

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}

class SnappyCompressionCodec(conf: Config) extends CompressionCodec {

  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException
  }

  val blockSize = Try(conf.getInt("hdm.io.compression.snappy.block.size")) getOrElse(32768)


  override def compress(bytes: Array[Byte]): Array[Byte] = {
    Snappy.compress(bytes)
  }

  override def uncompress(bytes: Array[Byte]): Array[Byte] = {
    Snappy.uncompress(bytes)
  }

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}