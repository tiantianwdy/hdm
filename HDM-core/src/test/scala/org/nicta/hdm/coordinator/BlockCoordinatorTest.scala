package org.nicta.hdm.coordinator

import com.baidu.bpit.akka.server.SmsSystem
import org.junit.{After, Before}
import org.nicta.hdm.executor.ClusterTestSuite
import org.nicta.wdy.hdm.executor.HDMContext
import org.junit.Test
import org.nicta.wdy.hdm.model.{HDM, DFM}

/**
 * Created by tiantian on 9/03/15.
 */
class BlockCoordinatorTest extends ClusterTestSuite {

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+")

  val text2 =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")


  def beforeTest(): Unit = {
    HDMContext.init(slots = 0) // start master
  }

  @Test
  def testAddRemoveBlock(): Unit = {
    HDMContext.startAsSlave(masterPath = "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
    val ddm = HDM.horizontal(text, text2) // register through hdm constrcutor
    Thread.sleep(1000)

    HDMContext.removeBlock(ddm.children.head.id)
    Thread.sleep(1000)
  }



  def afterTest(): Unit = {
    HDMContext.shutdown()
  }

}
