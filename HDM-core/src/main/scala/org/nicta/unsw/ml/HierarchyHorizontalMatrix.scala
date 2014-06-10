package org.nicta.unsw.ml

import org.nicta.wdy.hdm.Partitioner
import scala.collection._

/**
 * Created by Dongyao.Wu on 2014/5/30.
 */
class HierarchyHorizontalMatrix extends Matrix[Double] {

  private var data: Map[String, HorizontalSparseMatrix] = _

  private lazy val rowLength = data.toSeq.foldLeft(0L)((a, b) => a + b._2.rLength())


  def this(_data: Map[String, HorizontalSparseMatrix]) {
    this()
    this.data = _data
  }

  def this(_data: Seq[HorizontalSparseMatrix]) {
    this(_data.map {
      d => (hashCode().toString + "-" + _data.indexOf(d)) -> d
    }.toMap)
  }

  def indexes() = data.keySet


  def apply[B >: Double, U >: Double](f: (B) => U): HierarchyHorizontalMatrix = {
    new HierarchyHorizontalMatrix(data.map {
      kv =>
        kv._1 -> kv._2.apply(f)
    })
  }

  def map(f: (HorizontalSparseMatrix) => HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    new HierarchyHorizontalMatrix(data.map {
      kv =>
        kv._1 -> f(kv._2)
    })
  }
  

  def *[B >: Double](d: Double): HierarchyHorizontalMatrix = apply(_ * d)

  def /[B >: Double](d: Double): HierarchyHorizontalMatrix = apply(_ / d)

  def +[B >: Double](d: Double): HierarchyHorizontalMatrix = apply(_ + d)

  def -[B >: Double](d: Double): HierarchyHorizontalMatrix = apply(_ - d)


  def +[B >: Double](m: HierarchyHorizontalMatrix): HierarchyHorizontalMatrix = {
    val newData = (data.toSeq ++ m.data).groupBy(_._1).map{kv =>
      kv._1 -> kv._2.map(_._2).reduce{(a, b) => a + b}
    }
    new HierarchyHorizontalMatrix(newData)
  }

  def -[B >: Double](m: HierarchyHorizontalMatrix): HierarchyHorizontalMatrix = {
    val newData = (data.toSeq ++ m.data).groupBy(_._1).map(kv => kv._1 -> kv._2.foldLeft(new HorizontalSparseMatrix(0, Map.empty[String, Seq[Double]]))((a, b) => a - b._2))
    new HierarchyHorizontalMatrix(newData)
  }

  def *[B >: Double](m: HierarchyHorizontalMatrix): HierarchyHorizontalMatrix = {
    val newData = (data.toSeq ++ m.data).groupBy(_._1).map(kv => kv._1 -> kv._2.foldLeft(new HorizontalSparseMatrix(0, Map.empty[String, Seq[Double]]))((a, b) => a * b._2))
    new HierarchyHorizontalMatrix(newData)
  }

  def /[B >: Double](m: HierarchyHorizontalMatrix): HierarchyHorizontalMatrix = {
    val newData = (data.toSeq ++ m.data).groupBy(_._1).map(kv => kv._1 -> kv._2.foldLeft(new HorizontalSparseMatrix(0, Map.empty[String, Seq[Double]]))((a, b) => a / b._2))
    new HierarchyHorizontalMatrix(newData)
  }

  def +[B >: Double](m: HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    map(_ + m)
  }

  def -[B >: Double](m: HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    map(_ - m)
  }

  def *[B >: Double](m: HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    map(_ * m)
  }

  def /[B >: Double](m: HorizontalSparseMatrix): HierarchyHorizontalMatrix = {
    map(_ / m)
  }

  def dot(v: Seq[Double]) = {

    var reminder = rLength - v.length match {
      case l if l > 0 => v ++ DoubleFunc.seq(l)
      case l if l < 0 => v.take(rLength.toInt)
      case _ => v
    }

    new HierarchyHorizontalMatrix(data.map {
      m =>
        m._1 -> {
          val d = reminder.splitAt(m._2.rLength().toInt)
          reminder = d._2
          m._2 dot d._1
        }
    }).rSum
  }

  def dot(kv: Map[String, Seq[Double]]) = {
    new HierarchyHorizontalMatrix(data.map {
      m =>
        if (kv.contains(m._1)) m._1 -> {m._2 dot kv(m._1)}
        else m._1 -> m._2.rSum()
    }).rSum
  }

  def rSum = data.map(_._2).reduce((a,b) => a + b)

  def cSum = new HierarchyHorizontalMatrix(data.map(d => d._1 -> d._2.cSum()))

  def take(part:Int , total:Int, partitioner:Partitioner) : HierarchyHorizontalMatrix = {
    map(_.take(part,total, partitioner))
  }

  def take(parts:Seq[Int] , total:Int, partitioner:Partitioner) : HierarchyHorizontalMatrix = {
    map(_.take(parts,total, partitioner))
  }

  def rAppend(idx:String, mx : HorizontalSparseMatrix) : HierarchyHorizontalMatrix = {
    new HierarchyHorizontalMatrix(
//      if(data.contains(idx)) data + (idx -> (data(idx).rAppend(mx))) else // todo
        data + (idx->mx)
    )
  }

  def rExtract[B >: Double](mxKey:String, from: Long = 0L, to: Long): HorizontalSparseMatrix = {
     data.get(mxKey).map(_.rExtract(from,to)).getOrElse(null)
  }

  def rRemove(mxKey:String, idx:Int): HierarchyHorizontalMatrix = {
    if (data.contains(mxKey))
      new HierarchyHorizontalMatrix(
        if(data(mxKey).rLength()==1) data - mxKey
        else data + (mxKey -> data(mxKey).rRemove(idx))
      )
    else this
  }

  def collect() = {

    def addElemToMap(m: collection.mutable.Map[String, Seq[Double]], d: Map[String, Seq[Double]] ) = {
      for( (k,v) <- d){
        m += k -> (m.getOrElse(k, Seq.empty[Double]) ++ v)
      }
      m
    }
    val collected = collection.mutable.Map[String, Seq[Double]]()
    data.foreach(mx => addElemToMap(m = collected, d = mx._2.collect))
    collected

  }


  def rLength = rowLength

  def cLength = data.toList.map{_._2.cLength()}.max

  def toString(n: Int) = {
    data.map(kv => kv._1 + " ---->\n" + kv._2.toString(n)).mkString("\n")
  }

  def printData(n: Int) = {
    println(toString(n))
    this
  }

}
