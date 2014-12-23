package org.nicta.unsw.ml

import scala.collection._
import scala.Some
import scala.Some
import org.nicta.wdy.hdm.executor.Partitioner

/**
 * Created by Dongyao.Wu on 2014/5/29.
 */

trait Matrix[T] extends  Serializable{

}

class HorizontalSparseMatrix(val rowLength: Long) extends Matrix[Double] {

  def this(_size: Long, _data: Map[String, Seq[Double]]) {
    this(_size)
    this.data = _data
  }

  private var data: Map[String, Seq[Double]] = _


  def apply(c: String, r: Int) = {
    data.get(c) match {
      case Some(seq) => seq(r)
      case _ => 0D
    }
  }

  def apply(m: Int, n: Int): Double = {
    data.toSeq(m)._2(n)
  }

  def apply(c: String): Seq[Double] = {
    val vector = data.getOrElse(c, seq(rLength))
    rLength - vector.size match {
      case l if (l == 0) => vector
      case l if (l > 0) => vector ++ seq(l)
      case l => vector.take(rLength.toInt)
    }

  }

  def apply(r: Int) = {
    if(r < rowLength)
      new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> Seq(kv._2.apply(r))))
    else throw new RuntimeException()
  }


  def apply[B >: Double, U >: Double](f: (B) => U): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rowLength, data.map {
      t => t._1 -> t._2.map(d => f(d).asInstanceOf[Double])
    })
  }

  def filter[B >: Double](f:(B) => Boolean) : HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rowLength, data.filter{
       d => d._2.exists(f)
    } )
  }


  def *[B >: Double](m: Double): HorizontalSparseMatrix = apply(_ * m)

  def /[B >: Double](m: Double): HorizontalSparseMatrix = apply(_ / m)

  def +[B >: Double](m: Double): HorizontalSparseMatrix = apply(_ + m)

  def -[B >: Double](m: Double): HorizontalSparseMatrix = apply(_ - m)


  def +(m: Seq[Double]): HorizontalSparseMatrix ={

    new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> opTwoSeq(kv._2, m, DoubleFunc.plus)))
  }

  def -(m: Seq[Double]): HorizontalSparseMatrix ={

    new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> opTwoSeq(kv._2, m, DoubleFunc.minus)))
  }

  def *(m: Seq[Double]): HorizontalSparseMatrix ={

    new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> opTwoSeq(kv._2, m, DoubleFunc.multiply)))
  }

  def /(m: Seq[Double]): HorizontalSparseMatrix ={

    new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> opTwoSeq(kv._2, m, DoubleFunc.divide)))
  }


  /**
   * using input function to reduce columns with another matrix
   * @param m
   * @param f
   * @param computeMatched
   * @return
   */
  private  def cReduce(m: HorizontalSparseMatrix, f:(Double,Double)=> Double, computeMatched:Boolean = false): HorizontalSparseMatrix ={
    val rLen = Math.max(m.rLength(), rLength())
    val temp = (data.toSeq ++ m.data.toSeq).groupBy(s => s._1)
    val newData = if(computeMatched) temp.filter{s => s._2.length > 1}  else temp
    new HorizontalSparseMatrix(rLen, newData.mapValues {
      v => v.reduce(opTwoColumn(_, _, op = f))._2
    })
  }



  def +[B >: Double](m: HorizontalSparseMatrix, computeMatched:Boolean = false): HorizontalSparseMatrix = {

//    def func(d1: Double, d2: Double): Double = d1 + d2
    if(m.cLength() == 1) this + m.data.head._2
    else cReduce(m ,DoubleFunc.plus)

  }

  def -[B >: Double](m: HorizontalSparseMatrix, computeMatched:Boolean = false): HorizontalSparseMatrix = {

    if(m.cLength() == 1) this - m.data.head._2
    else cReduce(m , DoubleFunc.minus, computeMatched)
  }

  def *[B >: Double](m: HorizontalSparseMatrix): HorizontalSparseMatrix = {

    if(m.cLength() == 1) this * m.data.head._2
    else cReduce(m ,DoubleFunc.multiply, true)

  }

  def /[B >: Double](m: HorizontalSparseMatrix): HorizontalSparseMatrix = {

    if(m.cLength() == 1) this / m.data.head._2
    else cReduce(m ,DoubleFunc.divide, true)

  }

  def dot[B >: Double](m: Seq[Double]): HorizontalSparseMatrix = {//todo change the input as an vertical matrix

    def func(d1: Double, d2: Double): Double = d1 * d2

    new HorizontalSparseMatrix(1, data.map(kv => kv._1 -> opTwoSeq(kv._2, m, func))).rSum()

  }

  def rSum(): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rLength(), data.map {
      kv => (kv._1, Seq(kv._2.sum))
    })
  }


  def cSum():HorizontalSparseMatrix = {
    def func(d1: Double, d2: Double): Double = d1 + d2
    new HorizontalSparseMatrix(rLength(), Map("_total" -> {data.map(_._2).reduce(opTwoSeq(_,_, func))}))
  }

  /**
   * extract one column from matrix
   * @param n
   * @tparam B
   * @return
   */
  def cExtract[B >: Double](n: Long): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rowLength, data.take(n.toInt))
  }


  /**
   * extract a subset of column from matrix
   * @param from
   * @param to
   * @tparam B
   * @return
   */
  def rExtract[B >: Double](from: Long = 0L, to: Long): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(to - from, data.map(kv => kv._1 -> kv._2.slice(from.toInt, to.toInt)))
  }


  def cAppend[B >: Double](m: HorizontalSparseMatrix): HorizontalSparseMatrix = {
    m.rLength() - this.rLength() match {
      case l if l == 0 => new HorizontalSparseMatrix(rowLength, data ++ m.data)
      case l if l > 0 => this.cAppend(m.rExtract(to = rowLength))
      case l if l < 0 => this.cAppend(m.rAppend(-l))
    }
  }

  def size() = cLength * rLength

  def cLength(): Long = data.size

  def rLength(): Long = rowLength


  def opTwoColumn(c1: (String, Seq[Double]), c2: (String, Seq[Double]), op: (Double, Double) => Double): (String, Seq[Double]) = {
    c1._1 -> opTwoSeq(c1._2, c2._2, op)
  }

  def opTwoSeq(t1: Seq[Double], t2: Seq[Double], op: (Double, Double) => Double): Seq[Double] = {
    //propagate vector with its first value if it is one dimension
    val nt2 = if(t2.length ==1 && t1.length>1) seq(t1.length, t2(0)) else t2
    t1.zipAll(nt2, 0D, 0D).map(d => op(d._1, d._2))
  }


  def rAppend[B >: Double](n: Long): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rowLength + n, data.map(kv => kv._1 -> (kv._2 ++ seq(n))))
  }

  def rAppend[B >: Double](m: HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    new HierarchyHorizontalMatrix(Seq(this,m))
  }

  def take(partition:Int , total:Int, partitioner:Partitioner): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rLength(), data.map{
      ks => ks._1.hashCode % total -> ks
    }.groupBy(_._1).apply(partition).map(_._2))
  }

  def take(partitions:Seq[Int] , total:Int, partitioner:Partitioner): HorizontalSparseMatrix = {
    new HorizontalSparseMatrix(rLength(), data.toList.map{
      ks => ks._1.hashCode % total -> ks
    }.filter(iks=> partitions.contains(iks._1)).map(_._2).toMap)
  }

  def rRemove(idx:Int):HorizontalSparseMatrix = {
    if(idx < rLength())
      new HorizontalSparseMatrix(rLength() -1, data.map{
        kv => kv._1 -> (kv._2.take(idx -1) ++ kv._2.takeRight(rLength.toInt - idx - 1))
      })
    else this
  }

  def cAvg() = cSum() / cLength().toDouble

  def rAvg() = rSum() / rLength().toDouble


  def cMax() = {
    def func(d1: Double, d2: Double): Double = Math.max(d1,d2)
    new HorizontalSparseMatrix(rLength(), Map("_total" -> {data.map(_._2).reduce(opTwoSeq(_,_, func))}))
  }

  def cMin()  ={
    def func(d1: Double, d2: Double): Double = Math.min(d1,d2)
    new HorizontalSparseMatrix(rLength(), Map("_total" -> {data.map(_._2).reduce(opTwoSeq(_,_, func))}))
  }


  def collect(): Map[String, Seq[Double]] = data

  def t[B >: Double](): HorizontalSparseMatrix = ??? //todo return vertical matrix

  def inverse(): Unit = ???

  def dot[B >: Double](m: HorizontalSparseMatrix): HorizontalSparseMatrix = ???

  def printData(n: Int = 0) =  {
//    {if(n>0) data.take(n) else data}.foreach(d => println(s"${d._1} -> ${d._2}"))
    println(toString(n))
    this
  }

  def toString(n: Int = 0) = {
    {if(n>0) data.take(n) else data}.map(d => s"${d._1} -> ${d._2}").mkString("\n")
  }

  def seq(n: Long, default: Double = 0D) = Seq.fill(n.toInt) {
    default
  }
}

object HorizontalSparseMatrix{

  def apply(path:String, sep:String, keyIdx:Int, rowIdx:Seq[Int]):HorizontalSparseMatrix =  {
    val data = DataParser.parseCsv(path, sep, keyIdx, rowIdx)
    new HorizontalSparseMatrix(rowIdx.length.toLong, data)
  }

  def apply(path:String, sep:String, keyIdx:Int, rowIdx:Range):HorizontalSparseMatrix =  {
    val data = DataParser.parseCsv(path, sep, keyIdx, rowIdx)
    new HorizontalSparseMatrix(rowIdx.length.toLong, data)
  }

  def apply(path:String, sep:String, keyIdx:Int, rowIdx:Int):HorizontalSparseMatrix =  {
    HorizontalSparseMatrix.apply(path, sep, keyIdx, Seq(rowIdx))
  }
}
