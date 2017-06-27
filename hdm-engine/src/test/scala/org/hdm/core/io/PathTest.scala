package org.hdm.core.io

import org.hdm.core.context.{HDMServerContext, HDMContext, AppContext}
import org.hdm.core.functions.NullFunc
import org.hdm.core.model.DDM
import org.hdm.core.planing.{DDMGroupUtils, PlanningUtils}
import org.hdm.core.scheduling.SchedulingTestData
import org.hdm.core.server.HDMEngine
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by tiantian on 4/01/15.
 */
class PathTest extends SchedulingTestData {

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


  val hDMContext = HDMServerContext.defaultContext
  implicit val hDMEntry = HDMEngine()

  val appContext = new AppContext()

  def generatePath(group:Int = 2, groupSize:Int = 530) = {

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
      Path("hdfs://127.1.0.200:9001/user/spark/benchmark/1node/rankings"),
      Path("akka.tcp://127.0.0.1:9001/user/smsMaster")
    )
    val target = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
//    src.map{p => Path.path2Int(p) - Path.path2Int(target)} foreach( println(_))
//    println(Path.path2Int(target))
//    println((256 << 8) - 1)
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
    hDMEntry.init()
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
    val grouped = PlanningUtils.groupPathByBoundary(paths, 160)
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


  def printGroupInfo(grouped:Seq[Seq[DDM[_, _]]]):Unit = {
    println(s"total group:${grouped.size}")
    grouped foreach{ ddms =>
      println(s"group tasks:${ddms.size}, groupTotalSize = ${if(ddms.nonEmpty) ddms.map(_.blockSize).reduce(_ + _) else 0}" )
      ddms.foreach(p => print(s"path: ${p.preferLocation} , " +
        s" Size: ${p.blockSize} ; "))
      println("")
    }
  }


  @Test
  def testDDMGroupByLocation() = {
    hDMEntry.init()
//    val path = "hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/"
//    val ddms = DataParser.explainBlocks(Path(path))
    val numOfWorker = 160
    val parallelism = 160
    val blockSizeRange = 128 * 1024
    val pathPool = initAddressPool(numOfWorker)
    val candidates = generateWorkers(pathPool).map(Path(_))
    val paths = generateInputPath(pathPool, 1067).map(Path(_))
    val ddms = paths.map{path =>
      val id = HDMContext.newLocalId()
      val blockSize = 128 * 1024 + Math.round(Math.random()) * blockSizeRange
      new DDM(location = path,
      preferLocation = path,
      func = new NullFunc[String],
      blockSize = blockSize.toLong,
      blocks = mutable.Buffer(hDMContext.localBlockPath + "/" + id),
      appContext = AppContext())
    }
    val weights = ddms.map(ddm => ddm.blockSize / 1024F)
    //    val grouped = PlanningUtils.groupDDMByBoundary(ddms, 160)
////    val grouped = Path.groupDDMByLocation(ddms, 4)
//    candidates.foreach(println(_))
    val start = System.currentTimeMillis()
    val grouped1 = PlanningUtils.groupDDMByMinminScheduling(ddms, candidates, hDMContext)
    val point1 = System.currentTimeMillis()
    val grouped2 = PlanningUtils.groupDDMByBoundary(ddms, parallelism, 0)
    val point2 = System.currentTimeMillis()
    val grouped3 = DDMGroupUtils.groupDDMByBoundary(ddms, weights, parallelism)
    val point3 = System.currentTimeMillis()
    println("=== Minmin Group ===")
    printGroupInfo(grouped1)
    println("=== Boundary Group ===")
//    printGroupInfo(grouped2)
    println("=== Boundary Group with Weights ===")
    printGroupInfo(grouped3)
    println(s"Time consumed: minmin:${point1 - start} ms. \n Boundary Group: ${point2 - point1} ms. \n Boundary Group with Weights: ${point3 - point2} ms.")
  }
}
