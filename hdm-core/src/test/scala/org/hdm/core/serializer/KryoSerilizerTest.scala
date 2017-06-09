package org.hdm.core.serializer

import io.netty.buffer.Unpooled
import org.hdm.core.examples.KVBasedPrimitiveBenchmark
import org.hdm.core.executor.HDMContext
import org.hdm.core.functions.ParMapFunc
import org.hdm.core.model.ParHDM
import org.hdm.core.storage.Block
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by tiantian on 27/05/15.
 */
class KryoSerilizerTest {

  val text =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")

  val data = ArrayBuffer.empty[String] ++= text

  val data2 = ArrayBuffer.fill[(String, Seq[Double])](1000000){
    ("test", Seq(1D))
  }

  val data3 = ArrayBuffer.fill[(String, mutable.Buffer[Double])](1000000){
    ("test", mutable.Buffer(1D))
  }

  val serilizer = new KryoSerializer(HDMContext.defaultConf).newInstance()

  @Test
  def testSerilizingBlock(): Unit ={

    val blk = Block("bk-001", data2)
    val buf = serilizer.serialize(blk)
    val nBlk = serilizer.deserialize[Block[_]](buf)

    nBlk.data.foreach(println(_))



  }

  @Test
  def testKypoSerializingEfficiency(): Unit = {
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
    val buf = Block.encodeToBuf(blk)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    val len = buf.readInt() - 4
    val nBuf = Unpooled.buffer(len)
    buf.readBytes(nBuf, 4, len)
    val nBlk = Block.decodeResponse(nBuf, HDMContext.DEFAULT_COMPRESSOR)
    val t3 = System.currentTimeMillis()
    println(s"encoded size : ${buf.array().length} bytes.")
    println(s"decode finished in ${t3 - t2} ms.")
  }

  @Test
  def testEncodeSize() ={
    val t1 = System.currentTimeMillis()
    val buf = serilizer.serialize(data3)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    println(s"encoded size : ${buf.array().length} bytes.")
  }

  @Test
  def testClosureSerialize(): Unit ={
    val map = new ParMapFunc[String, Int]((d) => d match {
      case s:String  => s.length
      case _ => 0
    })
    val t1 = System.currentTimeMillis()
    val buf = serilizer.serialize(map)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    val nBlk = serilizer.deserialize[ParMapFunc[String, Int]](buf)
    val t3 = System.currentTimeMillis()
    println(s"encoded size : ${buf.array().length} bytes.")
    println(s"decode finished in ${t3 - t2} ms.")
  }

  @Test
  def testHDMSerialize(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 1
    val len = 3
    //    val benchmark = new KVBasedPrimitiveBenchmark(context)
    val benchmark = new KVBasedPrimitiveBenchmark(context = context, kIndex = 0, vIndex = 1)
    val hdm = benchmark.testGroupBy(data,len, parallelism)

    val t1 = System.currentTimeMillis()
    val buf = serilizer.serialize(hdm)
    val t2 = System.currentTimeMillis()
    println(s"encode finished in ${t2 - t1} ms.")
    val desHDM = serilizer.deserialize[ParHDM[_, _]](buf)
    println(desHDM)
    val t3 = System.currentTimeMillis()
    println(s"encoded size : ${buf.array().length} bytes.")
    println(s"decode finished in ${t3 - t2} ms.")
  }
}
