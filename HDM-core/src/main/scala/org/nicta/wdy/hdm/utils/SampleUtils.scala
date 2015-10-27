package org.nicta.wdy.hdm.utils

import java.util.Random

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by tiantian on 26/10/15.
 */
object SampleUtils {


  def randomSampling[T:ClassTag](input:Seq[T], sampleSize:Int):Seq[T] = {
    val random = new Random(42)
    val size = input.size
    val res = new Array[T](sampleSize)
    for(i <- 0 to sampleSize -1){
      val nIdx = random.nextInt(size)
      res(i) = input(nIdx)
    }
    res
  }


}
