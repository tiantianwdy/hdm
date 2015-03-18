package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.functions.{ParallelFunction, NullFunc, ParUnionFunc, FlattenFunc}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by tiantian on 7/01/15.
 */
/**
 *
 */
trait PhysicalPlanner extends Serializable{

  def plan[I: ClassTag, R: ClassTag](input: Seq[HDM[_,I]], target: HDM[I, R], parallelism:Int):Seq[HDM[_, _]]
}




/**
 * This planner constructs the whole data flow of the job before any task is running
 * It assumes during execution, every computed partition of a DFM would follow the naming rule:
 * {id of the ith block of a dfm } := dfm.id + "_b" + i
 *
 */
class DefaultPhysicalPlanner(blockManager: HDMBlockManager, isStatic:Boolean) extends PhysicalPlanner {

  def getStaticBlockUrls(hdm :HDM[_,_]):Seq[String] = hdm match {
    case dfm: HDM[_,_] =>
      val bn = hdm.parallelism
      for (i <- 0 until bn) yield hdm.id + "_b" + i
    case ddm:DDM[_,_] => ddm.blocks
    case x => Seq.empty[String]
  }

  def getDynamicBlockUrls(hdm :HDM[_,_]):Seq[String] = {
    val h = if(hdm.blocks ne null) hdm
    else blockManager.getRef(hdm.id)
    h.blocks.map(url => Path(url).name)
  }

  override def plan[I: ClassTag, R: ClassTag](input: Seq[HDM[_, I]], target: HDM[I, R], defParallel:Int): Seq[HDM[_,_]] = target match {
    case ddm: DDM[_,R] =>
      Seq(ddm)
    case leafHdm:DFM[_,String] if(input == null || input.isEmpty) =>
      val children = DataParser.explainBlocks(leafHdm.location)
      if(leafHdm.keepPartition) {
        val func = target.func.asInstanceOf[ParallelFunction[String,R]]
        val mediator = children.map(ddm => new DFM(id = leafHdm.id + "_b" +children.indexOf(ddm), children = Seq(ddm), dependency = target.dependency, func = func, parallelism = 1, partitioner = target.partitioner))
        val newParent = new DFM(id = leafHdm.id, children = mediator.toIndexedSeq, func = new ParUnionFunc[R](), parallelism = mediator.size)
        children ++ mediator :+ newParent
      } else {
//        val blockLocations = children.map(hdm => Path(hdm.blocks.head))
//        val blockMap = children.map(c => (c.location.toString, c)).toMap
//        val groupPaths = Path.groupPathBySimilarity(blockLocations, defParallel)
//        val inputs = groupPaths.map(seq => seq.map(b => blockMap(b.toString)))

        // group by location similarity
        val inputs = Path.groupDDMByLocation(children, defParallel)
        val func = target.func.asInstanceOf[ParallelFunction[String,R]]

        val mediator = inputs.map(seq => new DFM(id = leafHdm.id + "_b" + inputs.indexOf(seq), children = seq, dependency = target.dependency, func = func, parallelism = 1, partitioner = target.partitioner))
        val newParent = new DFM(id = leafHdm.id, children = mediator.toIndexedSeq, func = new ParUnionFunc[R](), dependency = target.dependency, parallelism = defParallel, partitioner = target.partitioner)
        children ++ mediator :+ newParent
      }
    case dfm:DFM[I,R] =>
      val inputArray = Array.fill(defParallel){new ListBuffer[String]}
      for (in <- input) {
        val inputIds = if (isStatic) getStaticBlockUrls(in)
        else getDynamicBlockUrls(in)
        val groupedIds = if(in.dependency == OneToOne || in.dependency == NToOne){
          inputIds.groupBy(id => inputIds.indexOf(id) % defParallel).values.toIndexedSeq
        } else {
          val pNum = if(in.partitioner ne null) in.partitioner.partitionNum else 1
          for( subIndex <- 0 until pNum) yield {
            inputIds.map{bid => bid + "_p" + subIndex}
          }.toIndexedSeq
        }
        for(index <- 0 until groupedIds.size) inputArray(index % defParallel) ++= groupedIds(index)
      }
      val newInput = inputArray.map(seq => seq.map(pid => new DDM(id = pid, location = null, func = new NullFunc[I])))
      val pHdms = newInput.map(seq => dfm.copy(id = dfm.id + "_b" + newInput.indexOf(seq), children = seq.asInstanceOf[Seq[HDM[_, I]]], parallelism = 1))
      val newParent = new DFM(id = dfm.id, children = pHdms.toIndexedSeq, func = new ParUnionFunc[R], dependency = dfm.dependency, partitioner = dfm.partitioner, parallelism = defParallel)
      pHdms :+ newParent
  }
}
