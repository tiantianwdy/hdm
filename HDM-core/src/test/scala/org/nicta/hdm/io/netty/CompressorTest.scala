package org.nicta.hdm.io.netty

import java.nio.ByteBuffer
import org.junit.{After, Before,Test}

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyBlockServer
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 8/09/15.
 */
class CompressorTest {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()
  val compressor = HDMContext.defaultHDMContext.getCompressor()


  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  val data2 = ArrayBuffer.fill[(String, List[Double])](1000000){
    ("0xb601998146d35e06", List(1D))
  }

  @Test
  def testCompressDecompress(): Unit ={
    val blck = Block("blk-001",data)
    println(blck.id, + blck.size)
    val bytes = serializer.serialize(blck).array()
    val compressed = compressor.compress(bytes)
    val uncompressed = compressor.uncompress(compressed)
    val byteBuf = ByteBuffer.wrap(uncompressed)
    val nBlk = serializer.deserialize[Block[_]](byteBuf)

    println(nBlk.id, + nBlk.size)

  }
}
