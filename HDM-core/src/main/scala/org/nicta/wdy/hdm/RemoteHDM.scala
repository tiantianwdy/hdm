package org.nicta.wdy.hdm

/**
  * Created by Tiantian on 2014/5/26.
  */
class RemoteHDM[AnyVal](val elemsPath: String) extends HDM[AnyVal] {

   override def children = HDM.findRemoteHDM(elemsPath)

  override def shuffle(partitioner: Partitioner): HDM[AnyVal] = ???

  override def collect(): HDM[AnyVal] = ???

  override def flatMap[U](f: (AnyVal) => U): HDM[U] = ???

  override def isLeaf: Boolean = ???

  override def location: Location = ???

  override def distribution: Distribution = ???
}

