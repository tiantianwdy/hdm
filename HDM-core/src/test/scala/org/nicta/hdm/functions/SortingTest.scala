package org.nicta.hdm.functions

import java.util

import org.junit.Test

import scala.collection.mutable
import scala.util.Random

/**
 * Created by tiantian on 28/09/15.
 */
class SortingTest {

  def gererateData(n:Int)={
    Array.fill(n){
      Random.nextInt()
    }
  }


  @Test
  def testTimSort(): Unit ={
    val data = gererateData(1000000)
    val copy = data.clone().map(d => d.asInstanceOf[AnyRef]);

    val start = System.currentTimeMillis()
    util.Arrays.sort(data)
    val end = System.currentTimeMillis()
    print(s" time consumed for quick-sort: ${end - start} ms.")

    java.util.Arrays.sort(copy)
    val end2 = System.currentTimeMillis()
    print(s" time consumed for tim sort: ${end2 - end} ms.")
  }

  @Test
  def testCollectionSort(): Unit ={
    val data = gererateData(1000000)
    val copy = data.toBuffer

    val start = System.currentTimeMillis()
    util.Arrays.sort(data)
    val end = System.currentTimeMillis()
    println(s" time consumed for quick-sort: ${end - start} ms.")
    println(data.take(10).mkString("(", ",", ")"))

    val start2 = System.currentTimeMillis()
    val array2 = copy.toArray
    util.Arrays.sort(array2)
    val sorted = array2.toBuffer
//    val sorted = copy.sorted(Ordering[Int])
    val end2 = System.currentTimeMillis()
    println(sorted.take(10).mkString("(", ",", ")"))
    print(s" time consumed for Buffer sorting: ${end2 - start2} ms.")

  }

}
