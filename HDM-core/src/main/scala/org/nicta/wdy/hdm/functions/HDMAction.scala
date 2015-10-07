package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.{BufferedBlockIterator, Path}
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Created by tiantian on 6/10/15.
 */
object HDMAction {


  def compute(hdm:HDM[_,_], parallelism:Int): Future[HDM[_, _]] = {
    HDMContext.compute(hdm, parallelism)
  }



  def sample[A:ClassTag](hdm: HDM[_, A], proportion:Either[Double, Int]): Iterator[A] = {
    proportion match {
      case Left(percentage) =>
        val sampleFunc: Buf[A] => Buf[A] = (in:Buf[A]) => {
          val size = (percentage * in.size).toInt
          Random.shuffle(in).take(size)
        }
        collect(hdm.mapPartitions(sampleFunc))
      case Right(size) =>
        collect(hdm).take(size)
    }

  }


  def collect[A:ClassTag](hdm: HDM[_, _]*): Iterator[A] = new BufferedBlockIterator[A](hdm)

}