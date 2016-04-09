package org.nicta.wdy.hdm.model

/**
 * Created by tiantian on 1/03/16.
 */
trait Node[T] {

  def value:T

  def self = this

  def parents:Seq[Node[T]]

  def children:Seq[Node[T]]

  def depth:Int

  def isRoot:Boolean = {
    parents == null || parents.isEmpty
  }

  def isLeaf:Boolean = {
    parents == null || parents.isEmpty
  }

  def transform[U](map:T => U):Node[U]


  def findAll(f:Node[T] => Boolean):Option[Seq[Node[T]]] ={
    findAllChildren(f) match {
      case None => if(f(self)) Some(self +: Nil) else None
      case Some(seq) => if(f(self)) Some(self +: seq) else Some(seq)
    }
  }


  def findAllChildren(f:Node[T] => Boolean):Option[Seq[Node[T]]] ={
    if(children.isEmpty) None
    else {
      Some(children.map(_.findAll(f)).foldLeft(Seq.empty[Node[T]])((seq, d2) => d2 match {
        case None => seq
        case Some(s) => seq ++ s
      }))
    }
  }

  def findFirst(f:Node[T] => Boolean):Option[Node[T]] = {
    if(f(self)) Some(this)
    else if(children.isEmpty) None
    else children.map(_.findFirst(f)).flatten.headOption
  }




}
