package org.hdm.core.io

import org.hdm.akka.server.SmsSystem
import org.hdm.core.model.DDM
import org.hdm.core.planing.PlanningUtils

import scala.util.Try
import scala.collection.mutable.Buffer

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

  lazy val (host:String, port:Int) = {
    if(address.contains(":")){
      val tup = address.splitAt(address.indexOf(":"))
      (tup._1, tup._2.substring(1).toInt)
    } else ("", null)
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

  val NETTY = "netty://"

  val HTTP = "http://"

  val HTTPS = "https://"

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
//      val srcSeq = src.address.split("\\.|:|/")
//      val tarSeq = target.address.split("\\.|:|/")
      val srcSeq = src.host.toCharArray
      val tarSeq = target.host.toCharArray
      val tarLen = Math.min(tarSeq.length, srcSeq.length)
      var cur = 0
      while(cur < tarLen && srcSeq(cur) == tarSeq(cur)){
        cur += 1
      }
      tarLen / (cur + 0.01D)
    }
  }

  /**
   *
   * @param paths
   * @param targetSet
   * @return
   */
  def calculateDistance(paths: Seq[Path], targetSet: Seq[Path]):Seq[Double] = {
    paths.map{p => // for each path compute the distance vector of target Set
      val vec = targetSet.map(t =>Path.calculateDistance(p, t) )
      vec.sum // return total distance
    }
  }

  def calculateWeightedDistance(paths: Seq[Path], targetSet: Seq[Path], weights:Seq[Float]):Seq[Double] = {
    require(weights.length == targetSet.length)
    paths.map{p => // for each path compute the distance vector to target Set
      calculateMeanDistance(p, targetSet, weights )
    }
  }
  
  def calculateMeanDistance(path: Path, targets: Seq[Path], weights:Seq[Float]):Double = {
    require(weights.length == targets.length)
    val vector =  for(i <- 0 until targets.length) yield {
      weights(i) * Path.calculateDistance(path, targets(i))
    }
    vector.sum
  }

  /**
   * find out closest the path that has minimum distance to target Set
   * @param paths
   * @param targetSet
   * @return
   */
  def findClosestLocation(paths: Seq[Path], targetSet: Seq[Path]): Path = {
//    val weights = Seq.fill(paths.length)(1F)
    val distanceVec = calculateDistance(paths, targetSet)
    val minimum = distanceVec.min
    paths(distanceVec.indexOf(minimum))
  }

  /**
   *  find out the closet cluster for the given path.
   * @param path the given path
   * @param targetSet the matching clusters
   * @param weights weights of every cluster
   * @return the index of the closest cluster
   */
  def findClosestCluster(path: Path, targetSet: Seq[Seq[Path]], weights:Seq[Seq[Float]]): Int = {
    val distanceVec =  for(idx <- 0 until targetSet.length) yield {
      calculateMeanDistance(path, targetSet(idx), weights(idx))
    }
    val minimum = distanceVec.min
    distanceVec.indexOf(minimum)
  }



  def path2Int(p:Path):Long = {
    Try {
      val ipSegs = p.address.split("\\.|:|/").take(5).map(_.toLong)
//      println(s"${(ipSegs(0) << 24)} + ${(ipSegs(1) << 16)} + ${(ipSegs(2) << 8)} + ${ipSegs(3)} + ${ipSegs(4) & 255}")
//      println((ipSegs(0) << 24) + (ipSegs(1) << 16) + (ipSegs(2) << 8) + ipSegs(3) + ipSegs(4) & 255)
      (ipSegs(0) << 24) + (ipSegs(1) << 16) + (ipSegs(2) << 8) + ipSegs(3) + (ipSegs(4) & 255)
    } getOrElse(0)
  }

  def path2Int(p:String):Long = {
    path2Int(Path(p))
  }


}
