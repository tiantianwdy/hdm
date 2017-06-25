package org.hdm.core.coordinator

import org.hdm.core.executor.ClusterTestSuite
import org.hdm.core.model.HDM
import org.junit.{After, Before, Test}

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


  @Before
  def beforeTest(): Unit = {
    hDMContext.init(slots = 0) // start master
  }

  @Test
  def testAddRemoveBlock(): Unit = {
    hDMContext.startAsSlave(masterPath = "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
    val ddm = HDM.horizontal(appContext = appContext, hdmContext = hDMContext, text, text2) // register through hdm constrcutor
    Thread.sleep(1000)

    hDMContext.removeBlock(ddm.children.head.id)
    Thread.sleep(1000)
  }


  @After
  def afterTest(): Unit = {
    hDMContext.shutdown()
  }

}
