package org.nicta.wdy.hdm.io

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

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
  def getBlockLocations(path: Path):Seq[Path] = {
    val conf = new Configuration()
//    val uri = URI.create(path.protocol + path.address)
    conf.set("fs.default.name", path.protocol + path.address)
    //    conf.set("hadoop.job.ugi", "tiantian")
    val fs = FileSystem.get(conf)

    //todo check whether this path can involve the block index
    val fStatus = fs.listStatus(new HPath(path.relativePath))
    fStatus.map(s => Path(s.getPath.toString))

  }



}
