package org.nicta.wdy.hdm.model

import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.executor.{HDMContext, KeepPartitioner, Partitioner}
import org.nicta.wdy.hdm.functions.{NullFunc, ParallelFunction}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.storage.{Block, _}

import scala.collection.mutable.Buffer
import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/5/25.
 *
 * DDM: Distributed Data Matrix
 *
 */
class DDM[T: ClassTag, R:ClassTag](val id: String = HDMContext.newLocalId(),
                           val elems: Seq[T] = null,
                           val dependency: DataDependency = OneToOne,
                           val func: ParallelFunction[T, R] = null,
                           val distribution: Distribution = Horizontal,
                           val location: Path = Path(HDMContext.localBlockPath),
                           val preferLocation:Path = null,
                           var blockSize:Long = -1,
                           var isCache:Boolean = false,
                           val state: BlockState = Computed,
                           var parallelism:Int = 1,
                           val keepPartition:Boolean = true,
                           val partitioner: Partitioner[R] = new KeepPartitioner[R](1)) extends ParHDM[T, R] {



  def this() {
   this(elems = null)
  }


  override def andThen[U: ClassTag](hdm: ParHDM[R, U]): ParHDM[T, U] = {
    new DDM(hdm.id,
      null,
      hdm.dependency,
      this.func.andThen(hdm.func),
      distribution, location, null, blockSize, isCache,
      state, parallelism,
      this.keepPartition && hdm.keepPartition,
      hdm.partitioner)
  }

  def copy(id: String = this.id,
           children:Seq[HDM[T]] = null,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = null,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           blockSize:Long = this.blockSize,
           isCache:Boolean = this.isCache,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):ParHDM[T, R] = {

    val ddm = new DDM(id, null, dependency, func, distribution, location, preferLocation, blockSize, isCache, state, parallelism, keepPartition, partitioner)
    ddm.blocks.clear()
    ddm.blocks ++= this.blocks
    ddm
  }

  val blocks: Buffer[String] = Buffer(Path(HDMContext.localBlockPath) + "/" +id)


  val children: Seq[ParHDM[_,T]] = null


//  override def sample(size:Int = 10): Seq[String]={
////    val len = Math.min(elems.length, size)
//    val res = HDMBlockManager().getBlock(id).data.take(size)
//    res.map(_.toString)
//  }

}

object DDM {

//  def apply[T: ClassTag](id:String, elems: Iterator[T], broadcast:Boolean = false): DDM[T,T] = {
//    this.apply(id, elems.toSeq, broadcast)
//  }

  def apply[T: ClassTag](id:String, elems: Seq[T], broadcast:Boolean = false): DDM[T,T] = {
    val ddm = new DDM[T,T](id= id,
      func = new NullFunc[T],
      blockSize = Block.byteSize(elems),
      state = Computed,
      location = Path(HDMContext.localBlockPath + "/" + id),
      preferLocation = Path(SmsSystem.physicalRootPath))
    HDMContext.addBlock(Block(ddm.id, elems), false)
    if(broadcast)
      HDMContext.declareHdm(Seq(ddm))
    ddm
  }

  def apply[T: ClassTag](elems: Seq[T]): DDM[T,T] = {
    val id = HDMContext.newLocalId()
    this.apply(id, elems, false)
  }

  def apply[T: ClassTag](elems: Seq[Seq[T]], broadcast:Boolean = false): Seq[DDM[T, T]] = {
    val (ddms, blocks:Seq[Block[T]]) =
      elems.map { seq =>
        val id = HDMContext.newLocalId()
        val d = synchronized {
          //need to be thread safe for reflection
          new DDM[T, T](id = id,
            func = new NullFunc[T],
            blockSize = Block.byteSize(seq),
            state = Computed,
            location = Path(HDMContext.localBlockPath + "/" + id))
        }
        val bl = Block(id, seq)
        (d, bl)
      }.unzip
    HDMBlockManager().addAll(blocks) //todo change to use HDMContext
    if(broadcast)
      HDMContext.declareHdm(ddms)
    ddms
  }
}


