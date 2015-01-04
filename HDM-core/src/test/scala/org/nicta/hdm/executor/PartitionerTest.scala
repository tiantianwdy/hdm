package org.nicta.hdm.executor

import org.junit.Test
import org.nicta.wdy.hdm.executor.MappingPartitioner

/**
 * Created by tiantian on 2/01/15.
 */
class PartitionerTest {
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
  def testMapPartitioner(): Unit ={
    val pFunc = (t:String) => t.hashCode()
    val p = new MappingPartitioner[String](4, pFunc)
    p.split(text).foreach(println(_))

  }

}
