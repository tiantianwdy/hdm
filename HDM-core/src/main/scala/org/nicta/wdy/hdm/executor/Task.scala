package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.io.{HDMIOManager, DataParser, Path}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.functions.{ParallelFunction, DDMFunction_1, SerializableFunction}
import java.util.concurrent.{TimeUnit, Callable}
import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.reflect.{ClassTag, classTag}
import scala.util.{Success, Failure}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager, BlockManager, Block}


/**
 * Created by Tiantian on 2014/12/1.
 */
case class Task[I:ClassTag,R: ClassTag](appId:String,
                     taskId:String,
                     input:Seq[HDM[_, I]],
                     func:ParallelFunction[I,R],
                     dep:Dependency,
                     keepPartition:Boolean = true,
                     partitioner: Partitioner[R] = null,
                     createTime:Long = System.currentTimeMillis())
                     extends Serializable with Callable[Seq[DDM[_,R]]]{

  final val inType = classTag[I]

  final val outType = classTag[R]

  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)

  override def call() = try {
    if(dep == OneToOne || dep == OneToN)
      runSequenceTask()
    else
      runShuffleTask()
  } catch {
    case e : Throwable =>
      e.printStackTrace()
      throw e
  }


    def runShuffleTask():Seq[DDM[_,R]]  = {
      val blocks = input.map(ddm => DataParser.readBlock(ddm, true))

      val inputData = blocks.map(_.data.asInstanceOf[Seq[I]]).flatten
      val ouputData = func.apply(inputData)

      val ddms = if (partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
        Seq(DDM[R](taskId, ouputData))
      } else {
        partitioner.split(ouputData).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
      }
      ddms

    }


    def runSequenceTask():Seq[DDM[_,R]] = {
      //load input data
      val data = input.map { in =>
        /*val inputData = if (!HDMBlockManager().isCached(in.id)) {
          in.location.protocol match {
            case Path.AKKA =>
              //todo replace with using data parsers
              println(s"Asking block ${in.location.name} from ${in.location.parent}")
              val await = HDMIOManager().askBlock(in.location.name, in.location.parent) // this is only for hdm
              Await.result[Block[_]](await, maxWaitResponseTime) match {
                case data: Block[_] =>
//                  val resp = HDMBlockManager().getBlock(id)
//                  HDMBlockManager().removeBlock(id)
                  data
                case _ => throw new RuntimeException(s"Failed to get data from ${in.location.name}")
              }

            case Path.HDFS =>
              val bl = DataParser.readBlock(in.location)
              println(s"Read data size: ${bl.size} ")
              bl
          }
        } else {
          println(s"input data are at local: [${(taskId, in.id)}] ")
          val resp = HDMBlockManager().getBlock(in.id)
          //        HDMBlockManager().removeBlock(in.id)
          resp
        }*/
        val inputData = DataParser.readBlock(in, true)
        //apply function
        println(s"Input data preparing finished, the task starts running: [${(taskId, func)}] ")
        func.apply(inputData.data.asInstanceOf[Seq[I]])

      }.flatten
      //partition as seq of data
      val ddms = if(partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
        Seq(DDM[R](taskId, data))
      } else {
        partitioner.split(data).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
      }
      ddms
    }

    def loadData(data: HDM[_,_]) ={

    }


    def runWithInput(blks:Seq[Block[_]]) = try {
      //load input data
//      val inputData = input.flatMap(b => HDMBlockManager.loadOrCompute[I](b.id).map(_.data))
//      val inputData = input.flatMap(hdm => hdm.blocks.map(Path(_))).map(p => HDMBlockManager().getBlock(p.name).data.asInstanceOf[Seq[I]])
      val inputData = blks.map(_.data.asInstanceOf[Seq[I]])
      //apply function
      val data = func.apply(inputData.flatten)
      //partition as seq of data
      val ddms = if(partitioner == null || partitioner.isInstanceOf[KeepPartitioner[_]]) {
        Seq(DDM[R](taskId, data))
      } else {
        partitioner.split(data).map(seq => DDM(taskId + "_p" + seq._1, seq._2)).toSeq
      }
      ddms
      //    DFM(children = input, blocks = blocks.map(_.id), state = Computed, func = func)
    } catch {
      case e : Throwable =>
        e.printStackTrace()
        throw e
    }

}
