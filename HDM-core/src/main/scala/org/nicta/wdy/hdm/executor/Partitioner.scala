package org.nicta.wdy.hdm.executor

/**
 * Created by Tiantian on 2014/5/26.
 */
trait Partitioner {

  def split[T](data:Seq[T], num:Int, func: T => Int):Map[Int,Seq[T]]

}


class RandomPartitioner extends  Partitioner {

  override def split[T](data: Seq[T], num: Int, func: T => Int = null ): Map[Int,Seq[T]] = {

    val pData = data.grouped(num).toList
    pData.map(seq => pData.indexOf(seq) -> seq).toMap
  }
}

class MappingPartitioner extends  Partitioner {

  override def split[T](data: Seq[T], num: Int, func: T => Int):  Map[Int,Seq[T]] = {

    data.groupBy(func)
  }
}

class KeepPartitioner extends  Partitioner {

  override def split[T](data: Seq[T], num: Int, func: (T) => Int): Map[Int, Seq[T]] = ???
}
