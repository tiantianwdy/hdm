package org.nicta.wdy.hdm.model

import scala.reflect.runtime.universe._

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.storage._
import org.nicta.wdy.hdm.storage.Block
import java.util.UUID
import org.nicta.wdy.hdm.executor.HDMContext

/**
 * Created by Tiantian on 2014/5/25.
 *
 * DDM: Distributed Data Matrix
 *
 */
case class DDM[T: TypeTag](val id: String = HDMContext.newLocalId(),
                           val elems: Seq[T] = null,
                           val dependency: Dependency = OneToOne,
                           val func: ParallelFunction[Path, T] = null,
                           val distribution: Distribution = Horizontal,
                           val location: Location = Local,
                           val state: BlockState = Declared) extends HDM[Path, T] {

  val blocks: Seq[String] = Seq(this.id)

  val children: Seq[HDM[_,Path]] = null


  override def sample(size:Int = 10): Seq[String]={
//    val len = Math.min(elems.length, size)
    val res = HDMBlockManager().getBlock(id).data.take(size)
    res.map(_.toString)
  }

}

object DDM {

  def apply[T: TypeTag](elems: Seq[T]): DDM[T] = {
    val ddm = new DDM[T](state = Computed)
    HDMContext.addBlock(Block(ddm.id, elems))
    HDMContext.declareHdm(Seq(ddm))
    ddm
  }

  def apply[T: TypeTag](elems: Seq[Seq[T]]): Seq[DDM[T]] = {
    val (ddms, blocks) = elems.map{ seq =>
      val d = DDM[T](state = Computed)
      val bl = Block(d.id, seq)
      (d, bl)
    }.unzip
    HDMBlockManager().addAll(blocks) //todo change to use HDMContext
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