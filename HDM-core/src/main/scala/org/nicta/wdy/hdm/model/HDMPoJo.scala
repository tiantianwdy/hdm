package org.nicta.wdy.hdm.model

/**
 * Created by tiantian on 8/04/16.
 */
class HDMPoJo(val id: String,
              val name :String,
              val hdmType:String,
              val children: Seq[String],
              val dependency: String,
              val func: String,
              val blocks: Seq[String],
              val distribution: String,
              val location: String, // todo change name to path
              val preferLocation: String,
              val blockSize: Long,
              val state: String,
              val parallelism: Int,
              val keepPartition: Boolean,
              val partitioner: String,
              val isCache: Boolean,
              var depth: Int ,
              val outType: String
               ) extends Serializable {

  def this(hdm:HDM[_]){
    this(hdm.id,
      hdm.func.getClass.getSimpleName,
      hdm.getClass.getSimpleName,
      if(hdm.children == null) null else hdm.children.map(_.id),
      hdm.dependency.toString,
      hdm.func.getClass.getCanonicalName,
      hdm.blocks,
      hdm.distribution.toString,
      hdm.location.toString,
      if(hdm.preferLocation == null) null else hdm.preferLocation.toString,
      hdm.blockSize,
      hdm.state.toString,
      hdm.parallelism,
      hdm.keepPartition,
      hdm.partitioner.getClass.getCanonicalName,
      hdm.isCache,
      hdm.depth,
      hdm.outType.getClass.getCanonicalName
    )
  }

  def toURL = location.toString
}


object HDMPoJo {

  def apply(hdm:HDM[_]) = new HDMPoJo(hdm)

}
