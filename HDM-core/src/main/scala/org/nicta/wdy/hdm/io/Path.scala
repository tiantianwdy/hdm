package org.nicta.wdy.hdm.io

/**
 * Created by Tiantian on 2014/5/26.
 */
class Path(val protocol:String, val absPath:String) extends Serializable {


  lazy val name = if (absPath.contains("/")) absPath.substring(absPath.lastIndexOf("/") + 1)
                  else absPath

  lazy val parent = this.toString.dropRight(name.length)

  lazy val address :String = {
    if(absPath.startsWith("/")) "localhost"
    else if(absPath.contains("/")) {
      absPath.substring(0, absPath.indexOf("/"))
    } else ""
  }

  lazy val (host, port) = {
    if(address.contains(":"))
    address.splitAt(address.indexOf(":"))
  }

  def relativePath = {
    absPath.drop(address.length)
  }

  override def toString: String = protocol + absPath

}

object Path {

  val HDM = "hdm://"

  val FILE = "file://"

  val HDFS = "hdfs://"

  val THCHYON = "tychyon://"

  def apply(protocol:String, host:String, port:Int, localPath:String) = {
    val path = if(localPath.startsWith("/")) localPath
                else "/" + localPath
    new Path(protocol, s"$host:$port$path")
  }

  def apply(protocol: String, localPath:String) = {
    val path = if(localPath.startsWith("/")) localPath
    else "/" + localPath
    new Path(protocol, path)
  }

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
