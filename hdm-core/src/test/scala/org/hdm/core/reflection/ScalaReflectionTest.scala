package org.hdm.core.reflection

import org.junit.Test

/**
 * Created by Tiantian on 2014/11/14.
 */
class ScalaReflectionTest {

  val elems = Seq("1", "DY", "29", "late")

  case class Person(id:Int, name:String, age:Int, other:String)
  case class Student(id:Int, name:String, age:Int, other:String)

  @Test
  def testDelegate(){

    val person = new Delegate(elems, {d => Person(d(0).toInt, d(1), d(2).toInt ,d(3))})
    val student = person.applyDynamic("map")(p => Student(p.id, p.name, p.age, p.other))
    println(student)
    println(student.compute())
  }

}

class MuckDDM(val elems:Seq[String] ) {


  import scala.reflect.runtime.{universe => ru}

  def map[R](func: String => R){
    elems.map(func)
  }

  override def toString: String = {
    s"DDM[${elems.mkString(",")}}]"
  }
}

import scala.language.dynamics

class Delegate[T](val elems:Seq[String], serialFunc: Seq[String]=> T)  extends Dynamic{

  def applyDynamic[R](m : String)(func:T=>R):Delegate[R] = {
     def newFunc(elems:Seq[String]) = func(serialFunc(elems))
     new Delegate[R](elems, newFunc)
  }

  def compute():T = {
    serialFunc(elems)
  }

  override def toString: String = {
    /*val ru = scala.reflect.runtime.universe
    val method = ru.typeOf[serialFunc.type].declaration("apply").asMethod
    s"Delegate[${elems.mkString(",")}}]\nfunc:[${ru.runtimeMirror(getClass.getClassLoader).reflect().reflectMethod(method).symbol}}]" +
    s"params[${method.paramss}}]"*/
     ""
  }
}
