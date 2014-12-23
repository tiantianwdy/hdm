package org.nicta.hdm.executor

import org.nicta.wdy.hdm.model.HDM
import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext

/**
 * Created by Tiantian on 2014/12/10.
 */
class HDMPlanerTest {

  @Test
  def testWordCountPlan(){
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
    println(text.length)
    text.foreach(println(_))
    val hdm = HDM.horizontal(text, text2)
    val wordCount = hdm.map(d=> (d,1)).groupBy(_._1).map(t => (t._1, t._2.map(_._2))).reduce(("",Seq(0)))((t1,t2) => (t1._1, t1._2))
    val hdms = HDMContext.explain(wordCount)
    hdms.foreach(println(_))
  }

}
