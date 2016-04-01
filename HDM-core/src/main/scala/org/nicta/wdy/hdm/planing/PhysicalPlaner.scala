package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.functions.{ParallelFunction, NullFunc, ParUnionFunc, FlattenFunc}
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.scheduling.Scheduler
import org.nicta.wdy.hdm.storage.HDMBlockManager
import org.nicta.wdy.hdm.utils.Logging

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

  def planMultiDFM(inputs:Seq[Seq[AbstractHDM[_]]], target: AbstractHDM[_], defParallel:Int): Seq[AbstractHDM[_]]
}




/**
 * This planner constructs the whole data flow of the job before any task is running
 * It assumes during execution, every computed partition of a DFM would follow the naming rule:
 * {id of the ith block of a dfm } := dfm.id + "_b" + i
 *
 */
class DefaultPhysicalPlanner(blockManager: HDMBlockManager, isStatic:Boolean) extends PhysicalPlanner with Logging{

  def resourceManager = HDMContext.getServerBackend().resourceManager

  def getStaticBlockUrls(hdm: AbstractHDM[_]):Seq[String] = hdm match {
    case dfm: HDM[_,_] =>
      val bn = hdm.parallelism
      for (i <- 0 until bn) yield hdm.id + "_b" + i
//    case ddm:DDM[_,_] => ddm.blocks
    case x => Seq.empty[String]
  }

  def getDynamicBlockUrls(hdm: AbstractHDM[_]):Seq[String] = {
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
        val slots = Scheduler.getAllAvailableWorkers(resourceManager.getAllResources())
        val candidates = for( i <- 0 until slots.size) yield {
          Path(slots(i).toString + "/" + i) //generate different path for slots on the same worker
        }
        log.warn(s"candidate size: ${candidates.size}. Default parallel: $defParallel .")
        val inputs = if (candidates.size == defParallel) {
          log.warn("group input data by MinminScheduling.")
          PlanningUtils.groupDDMByMinminScheduling(children, candidates)
        } else {
          log.warn("group input data by DDM boundary.")
          PlanningUtils.groupDDMByBoundary(children, defParallel).asInstanceOf[Seq[Seq[DDM[String, String]]]]
        }
        val func = target.func.asInstanceOf[ParallelFunction[String,R]]

        val mediator = inputs.map(seq => new DFM(id = leafHdm.id + "_b" + inputs.indexOf(seq), children = seq, dependency = target.dependency, func = func, parallelism = 1, partitioner = target.partitioner))
        val newParent = new DFM(id = leafHdm.id, children = mediator.toIndexedSeq, func = new ParUnionFunc[R](), dependency = target.dependency, parallelism = defParallel, partitioner = target.partitioner)
        children ++ mediator :+ newParent
      }

    case dfm:DFM[I,R] if(dfm.children.forall(_.isInstanceOf[DDM[_, I]])) => //for computed DFM
      val children = dfm.children.map(_.asInstanceOf[DDM[_, I]])
      val inputs = PlanningUtils.groupDDMByBoundary(children, defParallel).asInstanceOf[Seq[Seq[DDM[_,I]]]]
      val func = target.func
      val mediator = inputs.map(seq => new DFM(id = dfm.id + "_b" + inputs.indexOf(seq), children = seq, dependency = target.dependency, func = func, parallelism = 1, partitioner = target.partitioner))
      val newParent = new DFM(id = dfm.id, children = mediator.toIndexedSeq, func = new ParUnionFunc[R](), dependency = target.dependency, parallelism = defParallel, partitioner = target.partitioner)
      children ++ mediator :+ newParent

    case dfm:DFM[I,R] =>
      val inputArray = Array.fill(defParallel){new ListBuffer[String]}
      for (in <- input) {
        val inputIds = if (isStatic) getStaticBlockUrls(in)
        else getDynamicBlockUrls(in)
        val groupedIds = if(in.dependency == OneToOne || in.dependency == NToOne){ // parallel reading
          inputIds.groupBy(id => inputIds.indexOf(id) % defParallel).values.toIndexedSeq
        } else { // shuffle reading
          val pNum = if(in.partitioner ne null) in.partitioner.partitionNum else 1
          for( subIndex <- 0 until pNum) yield {
//            inputIds.map{bid => bid + "_p" + subIndex}
            val dis = subIndex * pNum % inputIds.size
            PlanningUtils.seqSlide(inputIds, dis).map{bid => bid + "_p" + subIndex} // slide partitions to avoid network contesting in shuffle
          }.toIndexedSeq
        }
        for(index <- 0 until groupedIds.size) inputArray(index % defParallel) ++= groupedIds(index)
      }
      val newInput = inputArray.map(seq => seq.map(pid => new DDM(id = pid, location = null, func = new NullFunc[I])))
      val pHdms = newInput.map(seq => dfm.copy(id = dfm.id + "_b" + newInput.indexOf(seq), children = seq.asInstanceOf[Seq[HDM[_, I]]], parallelism = 1))
      val newParent = new DFM(id = dfm.id, children = pHdms.toIndexedSeq, func = new ParUnionFunc[R], dependency = dfm.dependency, partitioner = dfm.partitioner, parallelism = defParallel)
      pHdms :+ newParent



  }

  override def planMultiDFM(inputs:Seq[Seq[AbstractHDM[_]]], target: AbstractHDM[_], defParallel:Int): Seq[AbstractHDM[_]] = target match {
    case dualDFM:DualDFM[_, _, _] =>
      val typedDFM = dualDFM.asInstanceOf[DualDFM[dualDFM.inType1.type, dualDFM.inType2.type, dualDFM.outType.type]]
      val inputArray = Array.fill(defParallel){new ListBuffer[ListBuffer[String]]}
      inputArray.foreach{ seq =>
        for (i <- 1 to inputs.length) seq += ListBuffer.empty[String] // initialize input Array
      }
      for(input <- inputs){ //generate input blockIDs
        val inputIdx = inputs.indexOf(input)
        for(in <- input){
          val inputIds = if (isStatic) getStaticBlockUrls(in)
          else getDynamicBlockUrls(in)
          val groupedIds = if(in.dependency == OneToOne || in.dependency == NToOne){ // parallel reading
            inputIds.groupBy(id => inputIds.indexOf(id) % defParallel).values.toIndexedSeq
          } else { // shuffle reading
          val pNum = if(in.partitioner ne null) in.partitioner.partitionNum else 1
            for( subIndex <- 0 until pNum) yield {
              //            inputIds.map{bid => bid + "_p" + subIndex}
              val dis = subIndex * pNum % inputIds.size
              PlanningUtils.seqSlide(inputIds, dis).map{bid => bid + "_p" + subIndex} // slide partitions to avoid network contesting in shuffle
            }.toIndexedSeq
          }
          for(index <- 0 until groupedIds.size) {
            val buffer = inputArray(index % defParallel).apply(inputIdx)
            buffer ++= groupedIds(index)
          }
        }
      }
      val newInput1 = inputArray.map(seq => seq(0).map(pid => new DDM(id = pid, location = null, func = new NullFunc[dualDFM.inType1.type])))
      val newInput2 = inputArray.map(seq => seq(1).map(pid => new DDM(id = pid, location = null, func = new NullFunc[dualDFM.inType2.type])))
      val newInputs = newInput1.zip(newInput2)
      val pHdms = newInputs.map{ tup =>
        typedDFM.copy(id =  dualDFM.id + "_b" + newInputs.indexOf(tup),
            input1 = tup._1.asInstanceOf[Seq[AbstractHDM[dualDFM.inType1.type]]],
            input2 = tup._2.asInstanceOf[Seq[AbstractHDM[dualDFM.inType2.type]]],
            parallelism = 1)
      }
      val newParent = new DFM(id = typedDFM.id,
        children = pHdms.toIndexedSeq.asInstanceOf[Seq[AbstractHDM[dualDFM.outType.type]]],
        func = new ParUnionFunc[dualDFM.outType.type],
        dependency = typedDFM.dependency,
        partitioner = typedDFM.partitioner,
        parallelism = defParallel)
      pHdms :+ newParent
  }
}
