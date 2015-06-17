package org.nicta.hdm.planing

import org.junit.Test
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.planing.PlanningUtils

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
