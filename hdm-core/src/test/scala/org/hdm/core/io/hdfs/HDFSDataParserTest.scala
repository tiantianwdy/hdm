package org.hdm.core.io.hdfs

import org.hdm.akka.monitor.SystemMonitorService
import org.hdm.core.context.AppContext
import org.hdm.core.io.{DataParser, HDFSParser, Path}
import org.hdm.core.model.HDM
import org.hdm.core.planing.StaticPlaner
import org.hdm.core.server.HDMServerContext
import org.hdm.core.storage.Block
import org.junit.{Ignore, After, Before, Test}

/**
 * Created by tiantian on 25/12/14.
 */
@Ignore("Require local HDFS installed.")
class HDFSDataParserTest {

  val hDMContext = HDMServerContext.defaultContext

  val appContext = new AppContext()

  var start = 0L

  @Before
  def before(): Unit ={
    start = System.currentTimeMillis()
  }

  @Test
  def testGetBlockLocations{
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    println(path.protocol)
    println(path.absPath)
    println(path.address)
    println(path.relativePath)
    println(path.host)
    println(path.port)
    HDFSUtils.getBlockLocations(path)
  }

  @Test
  def testDataParser: Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    DataParser.explainBlocks(path, hDMContext).foreach(println(_))
  }

  @Test
  def testHdfsPlaning(): Unit ={
    hDMContext.init()
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    new StaticPlaner(hDMContext).plan(HDM(path), 4 ).physicalPlan.foreach(println(_))
  }

  @Test
  def testReadBlock(): Unit = {
    hDMContext.init()
    Thread.sleep(1000)

    val step = 1
    var jvmMem = SystemMonitorService.getJVMMemInfo
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    var locs = DataParser.explainBlocks(path, hDMContext)
    val blockCache = new java.util.HashMap[Int, Seq[Block[_]]]
    var idx = 0
    println("Jvm free space" + (jvmMem(2) - jvmMem(1) + jvmMem(0)))// free total max
    while (!locs.isEmpty){
      val start = System.currentTimeMillis()
      val  blocks = locs.take(step) map { ddm =>
        val data = new HDFSParser().readBlock(ddm.location, ClassLoader.getSystemClassLoader)
        data
      }
      locs = locs.drop(step)
      idx = idx + 1
      //    blocks foreach(b => println(b.size))
      println("total read size:" + blocks.map(b => b.size).sum)
      blockCache.put(idx, blocks)
      jvmMem = SystemMonitorService.getJVMMemInfo
      println("Jvm free space:" + (jvmMem(2) - jvmMem(1) + jvmMem(0)))
      blockCache.remove(idx - 1)
      val end = System.currentTimeMillis()
      println(s"time eclipse: ${end - start} ms.")
    }

  }

  @Test
  def testReadBatch(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    val locations = DataParser.explainBlocks(path, hDMContext).take(10).map(_.location)
    new HDFSParser().readBatch[String](locations) foreach (b => println(b.size))
  }

  @After
  def after(){
    val end = System.currentTimeMillis() - start
    println("Time taken:" + end)
  }
}
