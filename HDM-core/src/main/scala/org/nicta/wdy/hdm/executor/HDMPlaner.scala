package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.functions.{FlattenFunc, ParallelFunction, ParUnionFunc}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.storage.HDMBlockManager
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag}
import scala.util.Try

/**
 * Created by Tiantian on 2014/12/10.
 */

/**
 *
 */
trait HDMPlaner {

  def plan(hdm:HDM[_,_], parallelism:Int):Seq[HDM[_,_]]

}


/**
 *
 */
trait PhysicalPlanner {

  def plan[I: ClassTag, R: ClassTag](input: Seq[HDM[_,I]], target: HDM[I, R], parallelism:Int):Seq[HDM[_, _]]
}


/**
 *
 */
object LocalPlaner extends HDMPlaner{


  override def plan(hdm:HDM[_,_], parallelism:Int = 4):Seq[HDM[_,_]] = {
    dftAccess(hdm, parallelism, 1)
  }



  private def dftAccess(hdm:HDM[_,_], defParallel:Int, followingParallel:Int):Seq[HDM[_,_]]=  {
    val nh = {if(hdm.parallelism < 1) hdm.withParallelism(defParallel)
              else hdm}.withPartitionNum(followingParallel)
    if(hdm.children == null || hdm.children.isEmpty){
      Seq{nh}
    } else {
      val subHDMs = hdm.children.map( h => dftAccess(h, defParallel, nh.parallelism)).flatten
      subHDMs :+ nh
    }
  }
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
    case ddm:DDM[_] => ddm.blocks
    case x => Seq.empty[String]
  }

  def getDynamicBlockUrls(hdm :HDM[_,_]):Seq[String] = {
    val h = if(hdm.blocks ne null) hdm
            else blockManager.getRef(hdm.id)
    h.blocks.map(url => Path(url).name)
  }

  override def plan[I: ClassTag, R: ClassTag](input: Seq[HDM[_, I]], target: HDM[I, R], defParallel:Int): Seq[HDM[_,_]] = target match {
    case ddm: DDM[R] =>
      Seq(ddm)
    case leafHdm:DFM[Path,String] if(input == null || input.isEmpty) =>
      val children = DataParser.explainBlocks(leafHdm.location)
      if(leafHdm.keepPartition) {
        val mediator = children.map(ddm => new DFM(id = leafHdm.id + "_b" +children.indexOf(ddm), children = Seq(ddm), func = new FlattenFunc[String](), parallelism = 1))
        val newParent = new DFM(id = leafHdm.id, children = mediator, func = new ParUnionFunc[String](), parallelism = mediator.size)
        children ++ mediator :+ newParent
      } else {
//        val pNum = leafHdm.parallelism
        val inputs = children.grouped(defParallel) // todo change to group by location similarity
        val mediator = inputs.map(seq => new DFM(id = leafHdm.id + "_b" + inputs.indexOf(seq), children = seq, func = new ParUnionFunc[String](), parallelism = 1))
        val newParent = new DFM(id = leafHdm.id, children = mediator.toSeq, func = new ParUnionFunc[String](), parallelism = defParallel)
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
      val newInput = inputArray.map(seq => seq.map(pid => new DDM[I](id = pid, location = null)))
      val pHdms = newInput.map(seq => dfm.copy(id = dfm.id + "_b" + newInput.indexOf(seq), children = seq.asInstanceOf[Seq[HDM[_, I]]], parallelism = 1))
      val newParent = new DFM(id = dfm.id, children = pHdms, func = new ParUnionFunc[R], dependency = dfm.dependency, partitioner = dfm.partitioner, parallelism = defParallel)
      pHdms :+ newParent
      /*
      val inputIds = input.map{h =>
        if(isStatic) getStaticBlockUrls(h)
        else getDynamicBlockUrls(h)
      }.flatten
      if(dfm.keepPartition){ //no need for repartition
        val preNum = if(input.head.partitioner ne null)input.head.partitioner.partitionNum else 1
        val pHdms = for (pid <- inputIds) yield {
          val pInput = if(dfm.dependency == OneToOne){
            Seq(new DDM[I](id = pid, location = null))
//            Seq(blockManager.getRef(pid))
          } else { // the dependency is n to n
            val pIds = for( subIndex <- 0 to preNum) yield pid + "_p" + subIndex
            pIds.map(pid => new DDM[I](id = pid, location = null))
//            blockManager.getRefs(pIds)
          }
          DFM(id = pid, // create each partitioned DFM
            children = pInput.asInstanceOf[Seq[HDM[_,I]]],
            func = dfm.func.asInstanceOf[ParallelFunction[I, R]])
        }
        val newParent = DFM(id = dfm.id, children = pHdms, func = new ParUnionFunc[R])
        pHdms :+ newParent
      } else { // need repartition
        val curNum = dfm.partitioner.partitionNum
        val pHdms = if (dfm.dependency == OneToOne){
          val groupedIDs = inputIds.grouped(curNum)
          groupedIDs.map{seq =>
            val in = seq.map(pid => new DDM[I](id = pid, location = null))
            dfm.copy(children = in.asInstanceOf[Seq[HDM[_, I]]])
          }
        } else { // n to n
          for (index <- 1 to curNum) yield {
            val pIDs = for( bid <- inputIds) yield bid + "_p" + index
            val in = pIDs.map(pid => new DDM[I](id = pid, location = null))
            dfm.copy(children = in.asInstanceOf[Seq[HDM[_, I]]])
          }
        }
        val newParent = DFM(id = dfm.id, children = pHdms.toSeq, func = new ParUnionFunc[R])
        pHdms.toSeq :+ newParent
      }*/
  }
}
/**
 *
 */
@Deprecated
object ClusterPlaner extends HDMPlaner { // need to be execute on cluster leader


  override def plan(hdm:HDM[_,_],  parallelism:Int):Seq[HDM[_,_]] = {
    dftAccess(hdm)
  }

  private def dftAccess(hdm:HDM[_,_]):Seq[HDM[_,_]]=  {

    if(hdm.children == null || hdm.children.isEmpty){ //leaf nodes load data from data sources
      hdm match {
        case ddm :DDM[_] =>
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

object StaticPlaner extends HDMPlaner{

  val planer = new DefaultPhysicalPlanner(HDMBlockManager(), true)

  override def plan(hdm:HDM[_,_], maxParallelism:Int):Seq[HDM[_,_]] = {
    val explainedMap  = new java.util.HashMap[String, HDM[_,_]]()// temporary map to save updated hdms
    val logicPlan = LocalPlaner.plan(hdm, maxParallelism)
    logicPlan.map{ h =>
      val input = Try {h.children.map(c => explainedMap.get(c.id)).asInstanceOf[Seq[HDM[_,h.inType.type]]]} getOrElse  Seq.empty[HDM[_,h.inType.type]]
      val newHdms = planer.plan(input, h.asInstanceOf[HDM[h.inType.type, h.outType.type ]], h.parallelism)
      newHdms.foreach(nh => explainedMap.put(nh.id, nh))
      newHdms
    }
  }.flatten
}
