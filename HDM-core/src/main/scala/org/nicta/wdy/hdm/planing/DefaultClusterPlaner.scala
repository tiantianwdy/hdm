package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.functions.{ParallelFunction, ParUnionFunc}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model.{OneToOne, DFM, DDM, HDM}
import org.nicta.wdy.hdm.storage.HDMBlockManager

/**
 * Created by tiantian on 7/01/15.
 */
class DefaultClusterPlaner {

}

@deprecated(message = "replaced with StaticPlanner", since = "0.0.1")
object ClusterPlaner extends HDMPlaner { // need to be execute on cluster leader


  override def plan(hdm:HDM[_,_],  parallelism:Int):Seq[HDM[_,_]] = {
    dftAccess(hdm)
  }

  private def dftAccess(hdm:HDM[_,_]):Seq[HDM[_,_]]=  {

    if(hdm.children == null || hdm.children.isEmpty){ //leaf nodes load data from data sources
      hdm match {
        case ddm :DDM[_,_] =>
          Seq(ddm)
        case leafHdm:DFM[Path,String] =>
          val children = DataParser.explainBlocks(leafHdm.location)
          if(leafHdm.keepPartition) {
            val mediator = children.map(ddm => new DFM(children = Seq(ddm), func = new ParUnionFunc[String]()))
            val newParent = new DFM(id = leafHdm.id, children = mediator, func = new ParUnionFunc[String]())
            children ++ mediator :+ newParent
          } else {
            val pNum = leafHdm.partitioner.partitionNum
            val inputs = children.grouped(pNum) // todo change to group by location similarity
            val mediator = inputs.map(seq => new DFM(children = seq, func = new ParUnionFunc[String]()))
            val newParent = new DFM(id = leafHdm.id, children = mediator.toSeq, func = new ParUnionFunc[String]())
            children ++ mediator :+ newParent
          }
        case x => throw new Exception("unsupported hdm.")
      }
    } else { // explain non-leave nodes, apply function on existing hdm blocks
    val subHDMs = hdm.children.map( h => dftAccess(h)).flatten
      if(hdm.keepPartition){
        val pNum = subHDMs.map(h => h.partitioner.partitionNum).sum
        val pHdms = for (index <- 0 to pNum)  yield { // change to for each child
        val pId = hdm.id + "_p" + index
          val pInput = if(hdm.dependency == OneToOne){
            Seq(HDMBlockManager().getRef(pId))
          } else { // n to n
          val pIds = for( subIndex <- 0 to pNum) yield pId + "_p" + subIndex
            HDMBlockManager().getRefs(pIds)
          }
          new DFM(id = pId, // create each partition DFM
            children = pInput.asInstanceOf[Seq[HDM[_,hdm.inType.type ]]],
            func = hdm.func.asInstanceOf[ParallelFunction[hdm.inType.type, hdm.outType.type]])
        }
        val newParent = new DFM(id = hdm.id, children = pHdms, func = new ParUnionFunc[hdm.outType.type])
        subHDMs ++ pHdms :+ newParent
      } else {
        subHDMs :+ hdm
      }
    }
  }
}

