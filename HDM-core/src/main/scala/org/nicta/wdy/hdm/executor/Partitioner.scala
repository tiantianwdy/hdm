package org.nicta.wdy.hdm.executor

/**
 * Created by Tiantian on 2014/5/26.
 */
trait Partitioner[T] extends Serializable{

  var partitionNum:Int

  val pFunc: T => Int

  def split(data:Seq[T]):Map[Int,Seq[T]]

}


class RandomPartitioner[T](var partitionNum:Int, val pFunc: T => Int = null ) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int,Seq[T]] = {

    val pData = data.grouped(partitionNum).toList
    pData.map(seq => pData.indexOf(seq) -> seq).toMap
  }
}

class MappingPartitioner[T] (var partitionNum:Int, val pFunc: T => Int ) extends  Partitioner[T] {



  override def split(data: Seq[T]):  Map[Int,Seq[T]] = {
    data.groupBy(d=> Math.abs(pFunc(d)) % partitionNum)
  }
}

class KeepPartitioner[T](var partitionNum:Int , val pFunc: T => Int = null) extends  Partitioner[T] {

  override def split(data: Seq[T]): Map[Int, Seq[T]] = ???
}
