package org.hdm.core.model

import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor._
import org.hdm.core.functions.{NullFunc, ParallelFunction}
import org.hdm.core.io.Path
import org.hdm.core.storage.{Block, _}

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
                           val blocks: Buffer[String],
                           val distribution: Distribution = Horizontal,
                           val location: Path,
                           val preferLocation:Path = null,
                           var blockSize:Long = -1,
                           var isCache:Boolean = false,
                           val state: BlockState = Computed,
                           var parallelism:Int = 1,
                           val keepPartition:Boolean = true,
                           val partitioner: Partitioner[R] = new KeepPartitioner[R](1),
                           val appContext: AppContext) extends ParHDM[T, R] {



//  def this() {
//   this(elems = null)
//  }


  override def andThen[U: ClassTag](hdm: ParHDM[R, U]): ParHDM[T, U] = {
    new DDM(hdm.id,
      null,
      hdm.dependency,
      this.func.andThen(hdm.func),
      this.blocks,
      distribution,
      location,
      preferLocation,
      blockSize,
      isCache,
      state, parallelism,
      this.keepPartition && hdm.keepPartition,
      hdm.partitioner, appContext)
  }

  def copy(id: String = this.id,
           children:Seq[HDM[T]] = null,
           dependency: DataDependency = this.dependency,
           func: ParallelFunction[T, R] = this.func,
           blocks: Seq[String] = this.blocks,
           distribution: Distribution = this.distribution,
           location: Path = this.location,
           preferLocation:Path = this.preferLocation,
           blockSize:Long = this.blockSize,
           isCache:Boolean = this.isCache,
           state: BlockState = this.state,
           parallelism: Int = this.parallelism,
           keepPartition: Boolean = this.keepPartition,
           partitioner: Partitioner[R] = this.partitioner):ParHDM[T, R] = {

    val ddm = new DDM(id, null,
      dependency,
      func,
      blocks.toBuffer, distribution, location, preferLocation, blockSize,
      isCache, state, parallelism, keepPartition, partitioner, appContext)
    ddm.blocks.clear()
    ddm.blocks ++= this.blocks
    ddm
  }


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

  def apply[T: ClassTag](id:String, elems: Seq[T], appContext:AppContext, blockContext:BlockContext, hdmContext:HDMContext = HDMContext.defaultHDMContext,   broadcast:Boolean = false): DDM[T,T] = {
    val context = if(hdmContext == null) HDMContext.defaultHDMContext
     else hdmContext
    val ddm = new DDM[T,T](id= id,
      func = new NullFunc[T],
      blocks = Buffer(blockContext.localBlockPath + "/" + id),
      blockSize = Block.byteSize(elems),
      state = Computed,
      location = Path(blockContext.localBlockPath + "/" + id),
      preferLocation = Path(SmsSystem.physicalRootPath),
      appContext = appContext)
    context.addBlock(Block(ddm.id, elems), false)
    if(broadcast)
      context.declareHdm(Seq(ddm))
    ddm
  }

  def apply[T: ClassTag](elems: Seq[T], hdmContext:HDMContext, appContext:AppContext): DDM[T,T] = {
    val id = HDMContext.newLocalId()
    this.apply(id, elems, appContext, hdmContext.blockContext, hdmContext,  false)
  }

  def apply[T: ClassTag](elems: Seq[Seq[T]], hdmContext:HDMContext, appContext:AppContext,  blockContext:BlockContext, broadcast:Boolean = false): Seq[DDM[T, T]] = {
    val context = if(hdmContext == null) HDMContext.defaultHDMContext
    else hdmContext
    val (ddms, blocks:Seq[Block[T]]) =
      elems.map { seq =>
        val id = HDMContext.newLocalId()
        val d = synchronized {
          //need to be thread safe for reflection
          new DDM[T, T](id = id,
            func = new NullFunc[T],
            blocks = Buffer(blockContext.localBlockPath + "/" + id),
            blockSize = Block.byteSize(seq),
            state = Computed,
            location = Path(blockContext.localBlockPath + "/" + id),
            appContext = appContext)
        }
        val bl = Block(id, seq)
        (d, bl)
      }.unzip
    HDMBlockManager().addAll(blocks) //todo change to use HDMContext
    if(broadcast)
      context.declareHdm(ddms)
    ddms
  }

  def sources[T:ClassTag](urls: Seq[Path], hdmContext:HDMContext, appContext:AppContext, broadcast:Boolean = false): Seq[DDM[T, T]] = {
    val context = if(hdmContext == null) HDMContext.defaultHDMContext
    else hdmContext
    val ddms =
      urls.map { url =>
        val id = HDMContext.newLocalId()
        val d = synchronized {
          //need to be thread safe for reflection
          new DDM[T, T](id = id,
            func = new NullFunc[T],
            blocks = Buffer(url.toString),
            blockSize = 1,
            state = Computed,
            location = url,
            appContext = appContext)
        }
        d
      }
    if(broadcast)
      context.declareHdm(ddms)
    ddms
  }
}


