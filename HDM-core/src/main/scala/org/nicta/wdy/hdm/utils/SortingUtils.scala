package org.nicta.wdy.hdm.utils

import scala.reflect.ClassTag

/**
 * Created by tiantian on 29/09/15.
 */
object SortingUtils {

  
  def mergeSorted[T:ClassTag](sorted:Array[T], anotherSorted:Array[T])(implicit ordering: Ordering[T]): Array[T] ={
    val len1 = sorted.length
    val len2 = anotherSorted.length
    val dest = new Array[T](len1 + len2 )
    var (i, j, preI, preJ, resIdx) = (0, 0, 0, 0, 0)
    while(i < len1 && j < len2) {
      preI = i;
      preJ = j;
      while(ordering.lteq(sorted(i), anotherSorted(j))) {
        i += 1
      }
      if(i > preI) {
        System.arraycopy(sorted, preI, dest, resIdx, i - preI)
        resIdx += (i - preI)
      }
      while(ordering.lteq(anotherSorted(j), sorted(i))){
        j +=1
      }
      if(j > preJ) {
        System.arraycopy(anotherSorted, preJ, dest, resIdx, i - preI)
        resIdx += (j - preJ)
      }
    }
    if(i < len1) System.arraycopy(sorted, i, dest, resIdx, len1 - i)
    else if (j < len2) System.arraycopy(anotherSorted, j, dest, resIdx, len2 - j)
    dest
  }

  def mergeSorted(sorted:Array[Int], anotherSorted:Array[Int]): Array[Int] ={
    val len1 = sorted.length
    val len2 = anotherSorted.length
    val dest = new Array[Int](len1 + len2 )
    var (i, j, preI, preJ, resIdx) = (0, 0, 0, 0, 0)
    while(i < len1 && j < len2) {
      preI = i;
      preJ = j;
      while( i < len1 && j < len2 && sorted(i) <= anotherSorted(j)) {
        i += 1
      }
      if(i > preI && preI < len1) {
        System.arraycopy(sorted, preI, dest, resIdx, i - preI)
        resIdx += (i - preI)
      }
      while(i < len1 && j < len2 && anotherSorted(j) <= sorted(i)){
        j += 1
      }
      if(j > preJ && preJ < len2) {
        System.arraycopy(anotherSorted, preJ, dest, resIdx, j - preJ)
        resIdx += (j - preJ)
      }
    }
    if(i < len1) System.arraycopy(sorted, i, dest, resIdx, len1 - i)
    else if (j < len2) System.arraycopy(anotherSorted, j, dest, resIdx, len2 - j)
    dest
  }
  
}
