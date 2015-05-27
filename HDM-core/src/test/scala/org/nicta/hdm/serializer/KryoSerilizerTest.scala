package org.nicta.hdm.serializer

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.serializer.KryoSerializer
import org.nicta.wdy.hdm.storage.Block

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

  val data2 = ArrayBuffer.fill[(String, Int)](10){
    ("test", 1)
  }

  val serilizer = new KryoSerializer(HDMContext.defaultConf).newInstance()

  @Test
  def testSerilizingBlock(): Unit ={

    val blk = Block("bk-001", data2)
    val buf = serilizer.serialize(blk)
    val nBlk = serilizer.deserialize[Block[_]](buf)

    nBlk.data.foreach(println(_))



  }
}
