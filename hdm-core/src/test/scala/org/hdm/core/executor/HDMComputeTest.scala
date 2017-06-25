package org.hdm.core.executor

import org.hdm.core.context.HDMAppContext
import org.hdm.core.model.HDM
import org.junit.{After, Before, Test}

/**
 * Created by Tiantian on 2014/12/16.
 */
class HDMComputeTest extends ClusterTestSuite {

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
  def beforeTest(): Unit ={
    hDMContext.clusterExecution.set(false)
    hDMContext.init()
    appContext.setMasterPath("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(1000)
  }

  @Test
  def HDMComputeTest(){

    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.util.{Failure, Success}

    val hdm = HDM.horizontal(appContext, hDMContext, text, text2)
    val wordCount = hdm.map(w => (w,1)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

    val future = wordCount.compute(1)
    future onComplete  {
      case Success(hdm) =>
//        hdm.asInstanceOf[HDM[_,_]].sample(10).foreach(println(_))
      case Failure(t) => t.printStackTrace()
    }
    Await.ready(future, maxWaitResponseTime)
  }

  @After
  def afterTest(): Unit ={
    hDMContext.shutdown(appContext)
  }

}
