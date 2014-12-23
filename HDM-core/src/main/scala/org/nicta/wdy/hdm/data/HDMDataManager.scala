package org.nicta.wdy.hdm.data

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by Tiantian on 2014/5/27.
 */
class HDMDataManager {

  lazy  val pathToHDMMap = new ConcurrentHashMap[String,HDM[_, _]]()

  def apply(path:String):HDM[_, _] = pathToHDMMap.get(path)

  def apply(path:Path):HDM[_, _] = pathToHDMMap.get(path.strPath())




}
