package org.nicta.unsw.ml

import scala.collection.Seq

/**
 * Created by Dongyao.Wu on 2014/5/30.
 */
trait Func extends Serializable{

}

object DoubleFunc extends Func{

  def plus (d1: Double, d2: Double): Double = d1 + d2

  def minus (d1: Double, d2: Double): Double = d1 - d2

  def multiply (d1: Double, d2: Double): Double = d1 * d2

  def divide (d1: Double, d2: Double): Double = if(d2 != 0) d1 / d2 else d1

  def seq(n: Long, default: Double = 0D) = Seq.fill(n.toInt) {
    default
  }

  def opTwoSeq(t1: Seq[Double], t2: Seq[Double], op: (Double, Double) => Double): Seq[Double] = {
    //propagate vector with its first value if it is one dimension
    val nt2 = if(t2.length ==1 && t1.length>1) seq(t1.length, t2(0)) else t2
    t1.zipAll(nt2, 0D, 0D).map(d => op(d._1, d._2))
  }
}
