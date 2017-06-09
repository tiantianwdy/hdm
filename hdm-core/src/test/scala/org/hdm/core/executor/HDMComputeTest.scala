package org.hdm.core.executor

import org.hdm.core.model.HDM
import org.junit.Test

/**
 * Created by Tiantian on 2014/12/16.
 */
class HDMComputeTest {

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

  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  @Test
  def HDMComputeTest(){

    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.util.{Failure, Success}

    hDMContext.init()
    val hdm = HDM.horizontal(appContext, hDMContext, text, text2)
    val wordCount = hdm.map(w => (w,1)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

    wordCount.compute(4) onComplete  {
      case Success(hdm) =>
//        hdm.asInstanceOf[HDM[_,_]].sample(10).foreach(println(_))
      case Failure(t) => t.printStackTrace()
    }
//    val hdms = HDMContext.explain(wordCount)
//    hdms.foreach(println(_))

//    Thread.sleep(1000)
  }

}
