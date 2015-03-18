package org.nicta.wdy.hdm.io

import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.model.DDM

import scala.util.Try

/**
 * Created by Tiantian on 2014/5/26.
 */
class Path(val protocol:String, val absPath:String, scope:String = "") extends Serializable {


  lazy val name = if (absPath.contains("/")) absPath.substring(absPath.lastIndexOf("/") + 1)
                  else absPath

  lazy val parent = this.toString.dropRight(name.length + 1)

  lazy val address :String = {
    if(absPath.startsWith("/")) "localhost"
    else if(absPath.contains("/")) {
      absPath.substring(0, absPath.indexOf("/"))
    } else absPath
  }

  lazy val (host, port) = {
    if(address.contains(":"))
    address.splitAt(address.indexOf(":"))
  }

  def relativePath = {
    absPath.drop(address.length)
  }

  override def toString: String = protocol + scope + absPath

}

object Path {

  val HDM = "hdm://"

  val AKKA = "akka.tcp://"

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
    val (scope, absPath) = if(tuple._2.contains("@")){
      val i = tuple._2.indexOf("@")
      tuple._2.splitAt(i+1)
    } else ("", tuple._2)
    new Path(tuple._1, absPath, scope)
  }

  def isLocal(path:String):Boolean = isLocal(Path(path))

  def isLocal(path: Path):Boolean = {
    //todo check local
    if(path.address == null || path.address.equals("")) true
    else path.address == Path(SmsSystem.physicalRootPath).address
  }


  def calculateDistance(src:Path, target:Path):Double = {
    
    if(src.address == target.address) 1
    else {
      val srcSeq = src.address.split("\\.|:|/")
      val tarSeq = target.address.split("\\.|:|/")
      val tarLen = tarSeq.length
      var cur = 0
      while(cur < srcSeq.size && srcSeq(cur)== tarSeq(cur)){
        cur += 1
      }
      tarLen / (cur + 0.01D)
    }
  }

  def calculateDistance(paths: Seq[Path], targetSet: Seq[Path]):Seq[Double] = {
    paths.map{p => // for each path compute the distance vector of target Set
      val vec = targetSet.map(t =>Path.calculateDistance(p, t) )
      vec.sum // return total distance
    }
  }

  /**
   * find out cloest the path that has minimum distance to target Set
   * @param paths
   * @param targetSet
   * @return
   */
  def findClosestLocation(paths: Seq[Path], targetSet: Seq[Path]): Path = {
    val distanceVec = calculateDistance(paths, targetSet)
    val minimum = distanceVec.min
    paths(distanceVec.indexOf(minimum))
  }

  def groupPathBySimilarity(paths:Seq[Path], n:Int) = {
    val avg = paths.size/n
    paths.sortWith( (p1,p2) => path2Int(p1) < path2Int(p2)).grouped(avg.toInt).toSeq
  }

  def groupDDMByLocation(ddms:Seq[DDM[String,String]], n:Int) = {
    val avg = ddms.size/n
    ddms.sortWith( (p1,p2) => path2Int(p1.preferLocation) < path2Int(p2.preferLocation)).grouped(avg.toInt).toSeq
  }

  def path2Int(p:Path):Long = {
    Try {
      val ipSegs = p.address.split("\\.|:|/").take(4).map(_.toLong)
      (ipSegs(0) << 24) + (ipSegs(1) << 16) + (ipSegs(2) << 8) + ipSegs(3)
    } getOrElse(0)
  }

  def path2Int(p:String):Long = {
    path2Int(Path(p))
  }


}
