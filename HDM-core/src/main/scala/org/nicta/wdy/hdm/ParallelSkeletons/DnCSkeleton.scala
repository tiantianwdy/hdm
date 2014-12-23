package org.nicta.wdy.hdm.ParallelSkeletons

import org.nicta.wdy.hdm.model.HDM

/**
 * Created by Tiantian on 2014/11/4.
 */
/**
 *
 * @tparam I  Input data type
 * @tparam M  merge phase data type
 * @tparam T  target data type
 */
trait DnCSkeleton[I,M,T] extends Skeleton{

  val divide: (I) => M

  val conquer: (M) => T

  val merge: (T,T) => T

}

trait MapSkeleton[I, T] extends Skeleton{

  def map(f: (I) => T): HDM[I,T]
}

trait MSSkeleton[I,M,T] extends MapSkeleton[I, M]{

  def reduce(f:(M,M) => T) : HDM[M,T]
}

trait MSRSkeleton[I,M,T] extends MSSkeleton[I,M,T]{

  def split(f:(M) => Seq[M]) : HDM[M,M]

}

/**
 *
 * @tparam I  Input data type
 * @tparam M  merge phase data type
 * @tparam T  target data type
 */
trait MMRSkeleton[I,M,T] extends MSSkeleton[I,M,T]{


  def merge(f:(M,M) => M) : HDM[M,M]


}

/**
 *
 * @tparam I  Input data type
 * @tparam M  mediate phase data type
 * @tparam T  target data type
 */
trait MRMSkeleton[I,M,T] extends MSSkeleton[I,M,T]{


  def merge(f:(T,T) => T) : HDM[T,T]
}


/**
 *
 * @tparam I  Input data type
 * @tparam T  target data type
 */
trait forSkeleton[I,T] extends Skeleton{

  def foreach(f: (I) => T) : HDM[I,T]
}

