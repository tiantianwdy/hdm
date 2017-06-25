package org.hdm.core.executor

import org.hdm.core.Arr
import org.hdm.core.context.HDMContext
import org.hdm.core.functions.{CoGroupFunc, ParMapAllFunc}
import org.hdm.core.model.{DDM, NToOne, ParHDM}
import org.hdm.core.server.HDMServerContext
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer
/**
 * Created by tiantian on 28/03/16.
 */
class DualInputTaskTest extends ClusterTestSuite {

  val data1 =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+").filter(_.nonEmpty).toIterator

  val data2 = Array(
    "a", "b", "c", "d", "e", "w", "l", "t"
  ).toIterator

  val data3 = ArrayBuffer.fill(100000){
    (Math.random() * 1000)
  }

  val data4 =  ArrayBuffer.fill(100000){
    (Math.random() * 10000)
  }

  val blkSize = 10


  def getNettyBlockLocations(prefix:String, blkSize:Int): Seq[String] ={
    val url = "netty://tiantian-HP-EliteBook-Folio-9470m:9091"
    for (i <- 0 until blkSize) yield {
      s"$url/$prefix-$i"
    }
  }

  @Before
  def beforeTest(): Unit ={
    hDMContext.init()
  }

  @Test
  def localDualInputTaskTest(): Unit ={
    implicit val executorContext = ClusterExecutorContext()

    val data1Prefix = "Blk1"
    val data2Prefix = "Blk2"
    val input1 = ArrayBuffer.empty[ParHDM[_,Double]]
    val input2 = ArrayBuffer.empty[ParHDM[_,Double]]
//    HDMBlockManager.initBlockServer()
    for(i <- 0 until blkSize){
      val id = s"$data1Prefix-$i"
      val id2 = s"$data2Prefix-$i"
      HDMBlockManager().add(id, Block(id, data3))
      HDMBlockManager().add(id2, Block(id2, data4))
      val ddm1 = DDM(id, data3, appContext, hDMContext.blockContext(), hDMContext)
      val ddm2 = DDM(id2, data4, appContext, hDMContext.blockContext(), hDMContext)
      HDMBlockManager().addRef(ddm1)
      HDMBlockManager().addRef(ddm2)
      input1 += ddm1
      input2 += ddm2
    }

    val groupFunc = (d:Double) => d.toInt % 100
    val func = new CoGroupFunc(groupFunc, groupFunc)

    val sampleFfunc = (arr:Arr[(Int, (Iterable[Double], Iterable[Double]))]) => arr.take(10)
    val nextFunc = new ParMapAllFunc(sampleFfunc)

    val composedFunc = func.andThen(nextFunc)

    val task = new TwoInputTask(appId = appContext.appName, version = appContext.version,
      taskId = HDMContext.newLocalId(), exeId = "ins-1",
      input1 = input1,
      input2 = input2,
      dep = NToOne,
      func = composedFunc,
      appContext = appContext,
      blockContext = hDMContext.blockContext())
    val start = System.currentTimeMillis()
    val res = task.call()
    val end = System.currentTimeMillis()
    res.foreach { ddm =>
      println(ddm.blockSize)
      HDMBlockManager()
        .getBlock(ddm.id)
        .data.asInstanceOf[Seq[(Int, (Iterable[_], Iterable[_]))]] foreach (kv =>
        println(kv._1 + "," + kv._2._1.head + "," + kv._2._2.head)
        )
    }
    println(s"Finished in ${end - start} ms.")

  }

  @After
  def after(): Unit ={
    hDMContext.shutdown(appContext)
  }
}
