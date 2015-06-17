package org.nicta.wdy.hdm.io.hdfs

import org.nicta.wdy.hdm.io.Path

/**
 * Created by tiantian on 16/06/15.
 *
 * meta info for a block of data
 * @param path the path for accessing the block
 * @param location the physical location of the data
 * @param size the size of the block
 */
case class BlockInfo(path:Path, location:Path, size:Long) extends  Serializable
