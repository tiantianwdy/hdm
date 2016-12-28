package org.hdm.core.math

/**
 * Created by Tiantian on 2014/5/23.
 */
trait Matrices[+T] extends Serializable {

  def rlength() : Long

  def cLength() : Long

  def + [B >: T](m : Matrices[B]) : Matrices[B]

  def + [B >: T](m : B) : Matrices[B]

  def - [B >: T](m : Matrices[B]) : Matrices[B]

  def - [B >: T](m : B) : Matrices[B]

  def * [B >: T](m : Matrices[B]) : Matrices[B]

  def * [B >: T](m : B) : Matrices[B]

  def / [B >: T](m : Matrices[B]) : Matrices[B]

  def / [B >: T](m : B) : Matrices[B]

  def t [B >: T]() : Matrices[B]

  def inverse()

  def length()

  def append[B >: T](m : Matrices[B]) : Matrices[B]

  def ++ [B >: T](m : Matrices[B]) = append(m)

  def lAppend[B >: T](m : Matrices[B]):Matrices[B]

  def :: [B >: T](m : Matrices[B]) = lAppend(m)

  def rExtract [B >: T](from :Long, to :Long) : Matrices[B]

  def cExtract [B >: T](from :Long, to :Long) : Matrices[B]

  def apply[B >:T,U >:T] (f: B=>U) : Matrices[U]

  def apply[B >: T](m:Int, n:Int): B

  def dot[B >: T](m : Matrices[B]) = lAppend(m)

}

