package org.hdm.core.executor

import org.junit.Test
import org.hdm.core.executor.HashPartitioner

import scala.util.Random

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


  val data = Seq.fill[Int](100){
    Random.nextInt(10000)
  }

  @Test
  def testMapPartitioner(): Unit ={
    val pFunc = (t:String) => t.hashCode()
    val p = new HashPartitioner[String](4, pFunc)
    p.split(text).foreach(println(_))

  }

  @Test
  def testRandomParititon(): Unit ={
    val p = new RandomPartitioner[Int](4)
    p.split(data).foreach(println(_))
  }

}
