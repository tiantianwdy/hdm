package org.nicta.hdm.io

import org.junit.Test
import org.nicta.hdm.scheduling.SchedulingTestData
import org.nicta.wdy.hdm.executor.{AppContext, HDMContext}
import org.nicta.wdy.hdm.functions.NullFunc
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model.DDM
import org.nicta.wdy.hdm.planing.PlanningUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Created by tiantian on 4/01/15.
 */
class PathTest extends SchedulingTestData{

  val datasource = Seq(
    Path("hdfs://17.110.0.1:9001/user/spark/benchmark/1node/rankings/part-00001"),
    Path("hdfs://189.0.0.10:9001/user/spark/benchmark/1node/rankings/part-00002"),
    Path("hdfs://189.210.30.104:9001/user/spark/benchmark/1node/rankings/part-00002"),
    Path("hdfs://127.10.1.2:9001/user/spark/benchmark/1node/rankings/part-00003"),
    Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00004"),
    Path("hdfs://17.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00005"),
    Path("hdfs://17.110.0.1:9001/user/spark/benchmark/1node/rankings/part-00001"),
    Path("hdfs://189.0.0.10:9001/user/spark/benchmark/1node/rankings/part-00002"),
    Path("hdfs://189.210.30.104:9001/user/spark/benchmark/1node/rankings/part-00002"),
    Path("hdfs://127.10.1.2:9001/user/spark/benchmark/1node/rankings/part-00003"),
    Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00004"),
    Path("hdfs://17.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00005")
  )

  val nodes = Seq(
    //      Path("akka.tcp://10.10.0.100:8999/user/smsMaster"),
    //      Path("akka.tcp://masterSys@10.10.0.100:8999/user/smsMaster"),
//    Path("akka.tcp://slaveSys@10.10.0.1:10010/user/smsMaster"),
//    Path("akka.tcp://slaveSys@10.10.0.1:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.20.0.2:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.20.0.2:8999/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.10.1:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.10.2:10020/user/smsMaster")
  )


  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  def generatePath(group:Int = 2, groupSize:Int = 57) = {

    val paths = ListBuffer.empty[Path]
    for{
      g <- 0 until group
      n <- 1 to groupSize
    } {
//      paths  += Path(s"hdfs://10.${(g +1) * 10}.0.${(Math.random()*255).toInt}:9001/user/spark/benchmark/1node/rankings/part-0000${g*groupSize + n}}")
      paths += Path(s"10.${(g +1) * 10}.0.${(Math.random()*255).toInt}:50010")
    }
    paths
  }

  @Test
  def testParseHostAndPort() = {
    val src = Seq(
      Path("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10010/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.100:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.10.1:10020/user/smsMaster"),
      Path("netty://127.0.0.1:9001/123213-2323"),
      Path("netty://127.0.0.1:9001/"),
      Path("netty://127.0.0.1:9001")
    )
    src.foreach(p => println(p.host + ":"  + p.port))
  }

  @Test
  def testComputeDistance(): Unit ={
    val src = Seq(
      Path("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10010/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.100:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.10.1:10020/user/smsMaster"),
      Path("akka.tcp://127.0.0.1:9001/user/smsMaster")
    )
    val target = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    src.map{p => Path.calculateDistance(p, target)} foreach( println(_))
  }

  @Test
  def testFindOutClosestPath(): Unit ={

    val targets = Seq(
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00001"),
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00002"),
      Path("hdfs://127.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00003"),
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00004"),
      Path("hdfs://127.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00005")
    )
    Path.calculateDistance(nodes, targets) foreach (println(_))
    println(Path.findClosestLocation(nodes, targets))
  }

  @Test
  def testIsLocalPath(): Unit ={
    hDMContext.init()
    val p = Path("akka.tcp://masterSys/user/smsMaster")
    println(Path.isLocal(p))

  }

  @Test
  def testPathGroupByDistance = {
    val paths = generatePath().toSeq
    val grouped = PlanningUtils.groupPathBySimilarity(paths, 15)
    grouped foreach{ seq =>
      println("group:")
      seq.foreach(p => print(s"path: ${p} , " +
        s" Value: ${Path.path2Int(p)} ; "))
      println("")
    }
//    grouped foreach{p =>
//      println(Path.findClosestLocation(nodes, p))
//      println(p)
//    }
  }

  @Test
  def testPathGroupByBoundary = {
    val paths = generatePath().toSeq
    val grouped = PlanningUtils.groupPathByBoundary(paths, 8)
    println(s"total group:${grouped.size}")
    grouped foreach{ seq =>
      println(s"group size:${seq.size}")
      seq.foreach(p => print(s"path: ${p} , " +
        s" Value: ${Path.path2Int(p)} ; "))
      println("")
    }
    //    grouped foreach{p =>
    //      println(Path.findClosestLocation(nodes, p))
    //      println(p)
    //    }
  }

  @Test
  def testPathSort(): Unit ={
    val paths = generatePath().toSeq
    paths.foreach(p =>
      println(s"path:${p}, " +
        s"value:${Path.path2Int(p)}"))
  }

  @Test
  def testDDMGroupByLocation() = {
    hDMContext.init()
//    val path = "hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/"
//    val ddms = DataParser.explainBlocks(Path(path))
    val numOfWorker = 20
    val blockSizeRange = 128
    val pathPool = initAddressPool(numOfWorker)
    val candidates = generateWorkers(pathPool).map(Path(_))
    val paths = generateInputPath(pathPool, 1067).map(Path(_))
    val ddms = paths.map{path =>
      val id = HDMContext.newLocalId()
      new DDM(location = path,
      preferLocation = path,
      func = new NullFunc[String],
      blockSize = 128*1000 + 1L,
      blocks = mutable.Buffer(HDMContext.defaultHDMContext.localBlockPath + "/" + id),
      appContext = AppContext())
    }
//    val grouped = PlanningUtils.groupDDMByBoundary(ddms, 160)
////    val grouped = Path.groupDDMByLocation(ddms, 4)
//    candidates.foreach(println(_))
    val grouped = PlanningUtils.groupDDMByMinminScheduling(ddms, candidates, hDMContext)
        println(s"total group:${grouped.size}")
        grouped foreach{ddm =>
          println(s"group tasks:${ddm.size}, groupTotalSize = ${ddm.map(_.blockSize).reduce(_ + _)}" )
          ddm.foreach(p => print(s"path: ${p.preferLocation} , " +
            s" Size: ${p.blockSize} ; "))
          println("")
        }
  }
}
