package org.nicta.hdm.io

import org.junit.{Before, After, Test}
import org.nicta.wdy.hdm.executor.{StaticPlaner, ClusterPlaner, HDMContext}
import org.nicta.wdy.hdm.io.{HdfsParser, DataParser, HDFSUtils, Path}
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 25/12/14.
 */
class HDFSDataParserTest {

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
    DataParser.explainBlocks(path).foreach(println(_))
  }

  @Test
  def testHdfsPlaning(): Unit ={
    HDMContext.init()
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    StaticPlaner.plan(HDM(path), 4 ).foreach(println(_))
  }

  @Test
  def testReadBlock(): Unit = {
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    DataParser.explainBlocks(path).take(10) map { ddm =>
      val data = new HdfsParser().readBlock(ddm.location)
      data
    } foreach(b => println(b.size))
  }

  @Test
  def testReadBatch(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    val locations = DataParser.explainBlocks(path).take(10).map(_.location)
    new HdfsParser().readBatch(locations) foreach (b => println(b.size))
  }

  @After
  def after(){
    val end = System.currentTimeMillis() - start
    println("Time taken:" + end)
  }
}
