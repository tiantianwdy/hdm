package org.nicta.wdy.hdm.model

import scala.reflect.runtime.universe._

/**
 * Created by Tiantian on 2014/12/11.
 */
class PairHDM[K:TypeTag,V:TypeTag](self:HDM[_,(K,V)]){

  def mapValue[R:TypeTag](f: V => R):HDM[(K,V), (K,R)] = {
    self.map(t => (t._1, f(t._2)))
  }

  def mapKey[NK:TypeTag] (f: K => NK):HDM[(K,V), (NK,V)] = {
    self.map(t => (f(t._1), t._2))
  }

  def reduceByKey(f: (V,V)=> V): HDM[_, (K,V)] = {
    self.reduceByKey(_._1, (d1,d2) => (d1._1,f(d1._2,d2._2))).map(_._2)
  }

  def swap():HDM[(K,V), (V,K)] ={
    self.map(t => (t._2, t._1))
  }

}
