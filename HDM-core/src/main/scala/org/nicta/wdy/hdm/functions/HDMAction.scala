package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.Arr
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.{BufferedBlockIterator, Path}
import org.nicta.wdy.hdm.model.{HDM, ParHDM$}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by tiantian on 6/10/15.
 */
object HDMAction {


  def compute(hdm:HDM[_], parallelism:Int): Future[HDM[_]] = {
    HDMContext.compute(hdm, parallelism)
  }



  def sample[A:ClassTag](hdm: HDM[A], proportion:Either[Double, Int]): Iterator[A] = {
    proportion match {
      case Left(percentage) =>
        val sampleFunc: Arr[A] => Arr[A] = (in:Arr[A]) => {
          val size = (percentage * in.size).toInt
          Random.shuffle(in).take(size)
        }
        traverse(hdm.mapPartitions(sampleFunc))
      case Right(size) =>
        traverse(hdm).take(size)
    }

  }


  def traverse[A:ClassTag](hdm: HDM[_]*): Iterator[A] = new BufferedBlockIterator[A](hdm)

}