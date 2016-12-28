package org.hdm.core.serializer

import com.typesafe.config.Config
import io.netty.buffer.Unpooled
import org.junit.Test
import org.hdm.core.collections.CompactBuffer
import org.hdm.core.executor.HDMContext
import org.hdm.core.serializer.JavaSerializer
import org.hdm.core.storage.Block

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
class JavaSerilizerTest {

  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  val data2 = ArrayBuffer.fill[Product2[String, Seq[Double]]](1000000){
    ("test", new ArrayBuffer[Double](2)) //initail size = 2
  }

  val data3 = ArrayBuffer.fill[(String, Seq[Double])](1000000){
    ("test", mutable.Buffer(1D))
  }

  val data4 = ArrayBuffer.fill[Product2[String, Double]](1000000){
    ("test",1D)
  }

  val data5 = new CompactBuffer[Product2[String, Double]] ++= data4

  val serilizer = new JavaSerializer(HDMContext.defaultConf).newInstance()

  @Test
  def testSerializingBlock(): Unit ={

    val blk = Block("bk-001", data2)
    val buf = serilizer.serialize(blk)
    val nBlk = serilizer.deserialize[Block[_]](buf)

    nBlk.data.take(10).foreach(println(_))



  }

  @Test
  def testJavaSerializingEfficiency(): Unit ={
    val blk = Block(HDMContext.newLocalId(), data4)
    //test direct serialization
    val t1 = System.currentTimeMillis()
    val buf = serilizer.serialize(blk)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    val nBlk = serilizer.deserialize[Block[_]](buf)
    val t3 = System.currentTimeMillis()
    println(s"encoded size : ${buf.array().length} bytes.")
    println(s"decode finished in ${t3 - t2} ms.")
  }

  @Test
  def testEncodeDecodeEfficiency(): Unit ={

    val blk = Block(HDMContext.newLocalId(), data2)
    //test direct serialization
    val t1 = System.currentTimeMillis()
    val buf = Block.encodeToBuf(blk)
    val t2 = System.currentTimeMillis()
    println(s"encode data ${buf.readableBytes()} bytes, finished in ${t2 - t1} ms.")
    val len = buf.readInt() - 4
    val buffer = buf.nioBuffer(4, len)
    val nBuf = Unpooled.wrappedBuffer(buffer)
    val resp = Block.decodeResponse(nBuf, HDMContext.DEFAULT_COMPRESSOR)
    val t3 = System.currentTimeMillis()
    println(s"decode size : ${resp.length} bytes,finished in ${t3 - t2} ms.")
    val respBlk = serilizer.deserialize[Block[_]](resp.data)
    val t4 = System.currentTimeMillis()
    println(s"deserialize data: ${respBlk.data.length}, finished in ${t4 - t2} ms.")

  }

  @Test
  def testEncodeSize() ={
    val t1 = System.currentTimeMillis()
    println(data5.bufferSize)
    val buf = serilizer.serialize(data4)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    println(s"encoded size : ${buf.array().length} bytes.")
  }



}
