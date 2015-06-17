package org.nicta.wdy.hdm.io.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.nicta.wdy.hdm.io.Path

/**
 * Created by tiantian on 24/12/14.
 */
object HDFSUtils {

   type HPath = org.apache.hadoop.fs.Path

  /**
   *
   * @param path
   * @return blocks under this path
   */
  def getBlockLocations(path: Path):Array[BlockInfo] = {
    val conf = new Configuration()
//    val uri = URI.create(path.protocol + path.address)
    conf.set("fs.default.name", path.protocol + path.address)
    //    conf.set("hadoop.job.ugi", "tiantian")
    val fs = FileSystem.get(conf)

    //todo check whether this path can involve the block index
    val fStatus = fs.listStatus(new HPath(path.relativePath))
    fStatus.map { s =>
      val blockLocations = fs.getFileBlockLocations(s, 0, s.getLen).head.getNames
      val blockSize = s.getBlockSize
//      (Path(s.getPath.toString), Path(blockLocations(0)))
      BlockInfo(Path(s.getPath.toString), Path(blockLocations(0)), blockSize)
    }
  }



}
