package org.nicta.hdm.functions

import java.util

import org.junit.Test
import org.nicta.wdy.hdm.functions.SortFunc
import org.nicta.wdy.hdm.utils.SortingUtils

import scala.collection.mutable
import scala.util.{Sorting, Random}

/**
 * Created by tiantian on 28/09/15.
 */
class SortingTest {

  def generateData(n:Int)={
    Array.fill(n){
      Random.nextInt()
    }
  }
  


  @Test
  def testTimSort(): Unit ={
    val data = generateData(1000000)
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
    val data = generateData(1000000)
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
  
  @Test
  def testSortFunctions(): Unit ={
    val data = generateData(1000000)
    val buf = data.toIterator
    val sorting = new SortFunc[Int]
    val start = System.currentTimeMillis()
//    util.Arrays.sort(data)
    Sorting.quickSort(data)
    val end = System.currentTimeMillis()
    println(s" time consumed for quick-sort: ${end - start} ms.")
    println(data.take(10).mkString("(", ",", ")"))

    val start2 = System.currentTimeMillis()
    val sorted = sorting(buf)
    val end2 = System.currentTimeMillis()
    println(sorted.take(10).mkString("(", ",", ")"))
    print(s" time consumed for Function sorting: ${end2 - start2} ms.")
  }

  @Test
  def testSortAggregation()={
    val batchNum = 500000
    val iter = 20
    val data = generateData(batchNum)
    val buf = data.toIterator
    val sorting = new SortFunc[Int]

    val inputs = for ( i <- 1 to iter) yield {
      generateData(batchNum)
    }
    val inputBufs = inputs.map(_.toIterator)
    val sortedInputs = inputs.map{ in =>
      val cloned = in.clone()
      Sorting.quickSort(cloned)
      cloned.toBuffer
    }
    // array sorting after contact
    val start = System.currentTimeMillis()
    var allInput:Array[Int] =  data
    for(arr <- inputs){
      allInput = Array.concat(allInput, arr)
    }
    Sorting.quickSort(allInput)
    val end = System.currentTimeMillis()
    println(s" time consumed for quick-sort: ${end - start} ms.")
    println(allInput.take(10).mkString("(", ",", ")"))

    // time for merge the sorted inputs
    val start2 = System.currentTimeMillis()
    var sorted = sorting(buf).toArray
    for(buffer <- inputBufs){
      sorted = SortingUtils.mergeSorted(buffer.toArray, sorted)
    }
    val res = sorted.toBuffer
    val end2 = System.currentTimeMillis()
    println(res.take(10).mkString("(", ",", ")"))
    println(s" time consumed for merge sorted array sorting: ${end2 - start2} ms.")

    val start3 = System.currentTimeMillis()
    var sorted3 = sorting(buf).toBuffer
    for(buffer <- inputBufs){
      sorted3 = sorting.aggregate(buffer, sorted3)
    }
    val end3 = System.currentTimeMillis()
    println(sorted.take(10).mkString("(", ",", ")"))
    println(s" time consumed for Aggregated sorting: ${end3 - start3} ms.")


  }

}
