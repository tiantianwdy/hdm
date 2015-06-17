package org.nicta.wdy.hdm.model

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.WeakTypeTag

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.functions.{NullFunc, ParallelFunction}
import org.nicta.wdy.hdm.storage._
import org.nicta.wdy.hdm.storage.Block
import org.nicta.wdy.hdm.executor.{KeepPartitioner, Partitioner, HDMContext}

/**
 * Created by Tiantian on 2014/5/25.
 *
 * DDM: Distributed Data Matrix
 *
 */
class DDM[T: ClassTag, R:ClassTag](val id: String = HDMContext.newLocalId(),
                           val elems: Seq[T] = null,
                           val dependency: DataDependency = OneToOne,
                           val func: ParallelFunction[T, R] = null,
                           val distribution: Distribution = Horizontal,
                           val location: Path = Path(HDMContext.localBlockPath),
                           val preferLocation:Path = null,
                           var blockSize:Long = -1,
                           val state: BlockState = Computed,
                           var parallelism:Int = 1,
                           val keepPartition:Boolean = true,
                           val partitioner: Partitioner[R] = new KeepPartitioner[R](1)) extends HDM[T, R] {




  def this(){
   this(elems = null)
  }


  override def andThen[U: ClassTag](hdm: HDM[R, U]): HDM[T, U] = {
    new DDM(hdm.id,
      null,
      hdm.dependency,
      this.func.andThen(hdm.func),
      distribution, location, null, blockSize,
      state, parallelism,
      this.keepPartition && hdm.keepPartition,
      hdm.partitioner)
  }

  def copy(id: String = this.id,
           children:Seq[HDM[_, T]] = null,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = null,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           blockSize:Long = -1,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):HDM[T, R] = {
    new DDM(id, null, dependency, func, distribution, location, preferLocation, blockSize, state, parallelism, keepPartition, partitioner)
  }

  val blocks: Buffer[String] = Buffer(Path(HDMContext.localBlockPath) + "/" +id)


  val children: Seq[HDM[_,T]] = null


  override def sample(size:Int = 10): Seq[String]={
//    val len = Math.min(elems.length, size)
    val res = HDMBlockManager().getBlock(id).data.take(size)
    res.map(_.toString)
  }

}

object DDM {

  def apply[T: ClassTag](id:String, elems: Seq[T], broadcast:Boolean = false): DDM[T,T] = {
    val ddm = new DDM[T,T](id= id,
      func = new NullFunc[T],
      state = Computed,
      location = Path(HDMContext.localBlockPath + "/" + id))
    HDMContext.addBlock(Block(ddm.id, elems), false)
    if(broadcast)
      HDMContext.declareHdm(Seq(ddm))
    ddm
  }

  def apply[T: ClassTag](elems: Seq[T]): DDM[T,T] = {
    val id = HDMContext.newLocalId()
    this.apply(id,elems)
  }

  def apply[T: ClassTag](elems: Seq[Seq[T]], broadcast:Boolean = false): Seq[DDM[T, T]] = {
    val (ddms, blocks:Seq[Block[T]]) =
      elems.map { seq =>
        val id = HDMContext.newLocalId()
        val d = synchronized {
          //need to be thread safe for reflection
          new DDM[T, T](id = id,
            func = new NullFunc[T],
            state = Computed,
            location = Path(HDMContext.localBlockPath + "/" + id))
        }
        val bl = Block(id, seq)
        (d, bl)
      }.unzip
    HDMBlockManager().addAll(blocks) //todo change to use HDMContext
    if(broadcast)
      HDMContext.declareHdm(ddms)
    ddms
  }
}


abstract class LeafValHDM(val elems: List[Double]) extends DoubleHDM{



/*  def this(elems: Array[Double]) {
    this(elems.toSeq)
  }*/
/*


  override def apply[B >: Double](m: Int, n: Int): B = elems.apply(m)


  override def map[U](f: (Double) => U): HDM[U] = HDM(elems.map(f))

  override def cExtract[B >: Double](from: Long, to: Long): Matrices[B] = {
    new LeafValHDM(elems.slice(from.toInt,to.toInt))
  }

  override def rExtract[B >: Double](from: Long, to: Long): Matrices[B] = this




  override def t[B >: Double](): Matrices[B] = this

  override def +[B >: Double](m: Matrices[B]): Matrices[B] = m match {
    case hdm : LeafValHDM if hdm.isLeaf => new LeafValHDM(hdm.elems.zip(elems).map{e => e._1 + e._2.toString.toDouble})
    case hdm : HDM[B] if !hdm.isLeaf => // todo computation propagating
     HDM[B]("path of new computed HDM")
  }

  override def +[B >: Double](m: B): Matrices[B] = apply[Double,Double]( _ + m.toString.toDouble)

  override def -[B >: Double](m: Matrices[B]): Matrices[B] = super.-(m)

  override def -[B >: Double](m: B): Matrices[B] = ???

  override def *[B >: Double](m: Matrices[B]): Matrices[B] = ???

  override def *[B >: Double](m: B): Matrices[B] = ???

  override def /[B >: Double](m: Matrices[B]): Matrices[B] = ???

  override def /[B >: Double](m: B): Matrices[B] = ???


  override def shuffle(partitioner: Partitioner): HDM[Double] = ???

  override def collect(): HDM[Double] = this

  override def flatMap[U](f: (Double) => U): HDM[U] = ???

  override def isLeaf: Boolean = ???

  override def location: Location = ???

  override def distribution: Distribution = ???

  override def children: List[HDM[Double]] = ???
*/
}