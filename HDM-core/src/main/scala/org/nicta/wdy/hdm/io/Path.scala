package org.nicta.wdy.hdm.io

/**
 * Created by Tiantian on 2014/5/26.
 */
case class Path(protocol:String, absPath:String) extends Serializable {

  override def toString: String = protocol + absPath
}

object Path {

  def apply(path:String) = {
    val idx = path.indexOf("://")

    val tuple = if(idx >=0) path.splitAt(idx + 3)
                else ("", path)
    new Path(tuple._1, tuple._2)
  }

  def isLocal(path:String):Boolean = isLocal(Path(path))

  def isLocal(path: Path):Boolean = {
    //todo check local
    true
  }
}
