package org.nicta.hdm.serializer

import com.typesafe.config.Config
import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.Block

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
    ("test", Seq(0D))
  }

  val data3 = ArrayBuffer.fill[(String, mutable.Buffer[Double])](1000000){
    ("test", mutable.Buffer(1D))
  }

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
    val blk = Block(HDMContext.newLocalId(), data2)
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
    val buf = Block.encode(blk)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    val nBlk = Block.decode(buf)
    val t3 = System.currentTimeMillis()
    println(s"encoded size : ${buf.array().length} bytes.")
    println(s"decode finished in ${t3 - t2} ms.")
  }

  @Test
  def testEncodeSize() ={
    val t1 = System.currentTimeMillis()
    val buf = serilizer.serialize(data2)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    println(s"encoded size : ${buf.array().length} bytes.")
  }



}
