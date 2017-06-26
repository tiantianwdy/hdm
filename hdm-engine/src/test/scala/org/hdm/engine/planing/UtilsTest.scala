package org.hdm.core.planing

import org.hdm.core.io.Path
import org.junit.Test

/**
 * Created by tiantian on 19/04/15.
 */
class UtilsTest {

  val nodes = Seq(
    Path("akka.tcp://10.10.0.100:8999/user/smsMaster"),
    Path("akka.tcp://masterSys@10.10.0.100:8999/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.0.1:10010/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.0.1:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.20.0.2:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.20.0.2:8999/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.10.1:10020/user/smsMaster"),
    Path("akka.tcp://slaveSys@10.10.10.2:10020/user/smsMaster")
  )

  @Test
  def seqSlidingTest(): Unit ={
    nodes foreach(println(_))

    for(i <- 1 to nodes.size){
      println(s"==== After sliding $i positions ====")
      PlanningUtils.seqSlide(nodes, i) foreach(println(_))
    }
  }

}
