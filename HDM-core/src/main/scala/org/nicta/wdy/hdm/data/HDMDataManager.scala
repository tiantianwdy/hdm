package org.nicta.wdy.hdm.data

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.nicta.wdy.hdm.HDM

/**
 * Created by Tiantian on 2014/5/27.
 */
class HDMDataManager {

  lazy  val pathToHDMMap = new ConcurrentHashMap[String,HDM[Any]]()

  def apply(path:String):HDM[Any] = pathToHDMMap.get(path)

  def apply(path:Path):HDM[Any] = pathToHDMMap.get(path.strPath())




}
