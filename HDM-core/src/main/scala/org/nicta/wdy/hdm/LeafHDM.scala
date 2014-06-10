package org.nicta.wdy.hdm

/**
 * Created by Tiantian on 2014/5/25.
 */
class LeafHDM {

}


class LeafValHDM(val elems: List[Double]) extends DoubleHDM{



  def this(elems: Array[Double]) {
    this(elems.toList)
  }


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
}