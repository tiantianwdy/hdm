package org.nicta.hdm.executor

import org.junit.Test
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.executor.HDMContext

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


  @Test
  def HDMComputeTest(){

    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.util.{Success, Failure}

    val hdm = HDM.horizontal(text, text2)
    val wordCount = hdm.map(w => (w,1)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

    wordCount.compute(4) onComplete  {
      case Success(hdm) => hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
      case Failure(t) => t.printStackTrace()
    }
//    val hdms = HDMContext.explain(wordCount)
//    hdms.foreach(println(_))
    HDMContext.init()
//    Thread.sleep(1000)
  }

}
