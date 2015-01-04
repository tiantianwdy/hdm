package org.nicta.hdm.io

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path

/**
 * Created by tiantian on 4/01/15.
 */
class PathTest {



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
    val srcs = Seq(
      Path("akka.tcp://127.0.0.1:8999/user/smsMaster"),
      Path("akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10010/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.1:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.2:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.0.2:8999/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.10.1:10020/user/smsMaster"),
      Path("akka.tcp://slaveSys@127.0.10.2:10020/user/smsMaster")
    )
    val targets = Seq(
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00001"),
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00002"),
      Path("hdfs://127.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00003"),
      Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings/part-00004"),
      Path("hdfs://127.0.0.2:9001/user/spark/benchmark/1node/rankings/part-00005")
    )
    Path.calculateDistance(srcs, targets) foreach (println(_))
    println(Path.findClosestLocation(srcs, targets))
  }

  @Test
  def testIsLocalPath(): Unit ={
    HDMContext.init()
    val p = Path("akka.tcp://masterSys/user/smsMaster")
    println(Path.isLocal(p))

  }
}
