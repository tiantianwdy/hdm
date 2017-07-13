package org.hdm.core.model


import org.hdm.core.Arr
import org.hdm.core.ConSeq
import org.hdm.core.context.{AppContext, HDMAppContext, HDMContext, HDMEntry}
import org.hdm.core.functions._
import org.hdm.core.io.Path
import org.hdm.core.storage.BlockState
import org.hdm.core.utils.{ClosureCleaner, SampleUtils}

import scala.concurrent.{Await, Future}
import scala.reflect.{ClassTag, classTag}
import scala.util.{Left, Random}
import scala.collection.JavaConversions._

/**
  * Created by Tiantian on 2014/5/23.
  *
  * HDM : Hierarchically Distributed Matrices
  */
abstract class HDM[R: ClassTag] extends Serializable {

  @transient
  var hdmContext: HDMContext = HDMContext.defaultContext

  @transient
  implicit val executionContext = hdmContext.executionContext //todo to changed to get from dynamic hdm context

  val appContext: AppContext

  val id: String

  val children: Seq[_ <: HDM[_]]

  val dependency: DataDependency

  val func: SerializableFunction[_, Arr[R]]

  def elements: Seq[_ <: HDM[_]] = children

  val blocks: Seq[String]

  val distribution: Distribution

  val location: Path // todo change name to path

  val preferLocation: Path

  var blockSize: Long

  val state: BlockState

  var parallelism: Int

  val keepPartition: Boolean

  val partitioner: Partitioner[R]

  var isCache: Boolean

  var depth: Int = 0

  // index of this hdm when partitioned
  var index: Int = 0

  def withIdx(idx: Int) = {
    index = idx
  }

  var isCompress: Boolean = false

  val outType = classTag[R]

  def toURL = location.toString

  def andThen[U: ClassTag](hdm: ParHDM[R, U]): HDM[U]


  def reduce[R1 >: R : ClassTag](f: (R1, R1) => R): ParHDM[R, R1] = {
    //parallel func is different with aggregation func
    ClosureCleaner(f)
    val parallel = new DFM[R, R](children = ConSeq(this),
      dependency = OneToOne,
      func = new ParReduceFunc[R, R](f),
      distribution = distribution,
      location = location,
      appContext = this.appContext)

    new DFM[R, R1](children = Seq(parallel),
      dependency = NToOne,
      func = new ParReduceFunc[R, R1](f),
      distribution = distribution,
      location = location,
      appContext = this.appContext).withParallelism(1).asInstanceOf[DFM[R, R1]]
  }


  //  functional operations

  def map[U: ClassTag](f: R => U): ParHDM[R, U] = {
    ClosureCleaner(f)
    new DFM[R, U](children = ConSeq(this),
      dependency = OneToOne,
      func = new ParMapFunc(f),
      distribution = distribution,
      location = location,
      appContext = this.appContext)
  }

  protected[hdm] def mapPartitions[U: ClassTag](mapAll: Arr[R] => Arr[U],
                                                dep: DataDependency = OneToOne,
                                                keepPartition: Boolean = true,
                                                partitioner: Partitioner[U] = new KeepPartitioner[U](1)): ParHDM[R, U] = {
    //    ClosureCleaner(mapAll)
    new DFM[R, U](children = ConSeq(this),
      dependency = dep,
      func = new ParMapAllFunc(mapAll),
      distribution = distribution,
      location = location,
      keepPartition = keepPartition,
      partitioner = partitioner,
      appContext = this.appContext)

  }

  protected[hdm] def mapPartitionsWithIdx[U: ClassTag](mapAll: (Long, Arr[R]) => Arr[U],
                                                       keepPartition: Boolean = true,
                                                       partitioner: Partitioner[U] = new KeepPartitioner[U](1)): ParHDM[R, U] = {
    //    ClosureCleaner(mapAll)
    new DFM[R, U](children = ConSeq(this),
      dependency = OneToOne,
      func = new ParMapWithIndexFunc(mapAll),
      distribution = distribution,
      location = location,
      keepPartition = keepPartition,
      partitioner = partitioner,
      appContext = this.appContext)

  }


  def filter(f: R => Boolean) = {
    //    ClosureCleaner(f)
    new DFM[R, R](children = ConSeq(this),
      dependency = OneToOne,
      func = new ParFindByFunc(f),
      distribution = distribution,
      location = location,
      appContext = this.appContext)
  }

  def groupBy[K: ClassTag](f: R => K): ParHDM[_, (K, Iterable[R])] = {
    ClosureCleaner(f)
    //todo add map side aggregation
    val pFunc = (t: R) => f(t).hashCode()
    val parallel = new DFM[R, R](children = ConSeq(this),
      dependency = OneToN,
      func = new NullFunc[R],
      distribution = distribution,
      location = location,
      keepPartition = true,
      partitioner = new HashPartitioner(4, pFunc),
      appContext = this.appContext)
    //    val parallel = this.copy(dependency = OneToN, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
    new DFM[R, (K, Iterable[R])](children = Seq(parallel),
      dependency = NToOne,
      func = new ParGroupByFunc(f),
      distribution = distribution,
      location = location,
      keepPartition = true,
      partitioner = new KeepPartitioner[(K, Iterable[R])](1),
      appContext = this.appContext)

  }


  def groupReduce[K: ClassTag](f: R => K, r: (R, R) => R): ParHDM[_, (K, R)] = {
    ClosureCleaner(f)
    ClosureCleaner(r)
    val pFunc = (t: (K, R)) => t._1.hashCode()
    val parallel = new DFM[R, (K, R)](children = ConSeq(this),
      dependency = OneToN,
      func = new ParReduceBy(f, r),
      distribution = distribution,
      location = location,
      keepPartition = false,
      partitioner = new HashPartitioner(4, pFunc),
      appContext = this.appContext)
    //    val groupReduce = (elems:Seq[(K,R)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toSeq
    new DFM[(K, R), (K, R)](children = Seq(parallel),
      dependency = NToOne,
      func = new ReduceByKey(r),
      distribution = distribution,
      location = location, keepPartition = true,
      partitioner = new KeepPartitioner[(K, R)](1),
      appContext = this.appContext)

  }

  def groupReduceByKey[K: ClassTag](f: R => K, r: (R, R) => R): ParHDM[_, (K, R)] = {
    ClosureCleaner(f)
    ClosureCleaner(r)
    val pFunc = (t: (K, R)) => t._1.hashCode()
    val mapAll = (elems: Arr[R]) => {
      elems.toSeq.groupBy(f).mapValues(_.reduce(r)).toIterator
    }
    val parallel = new DFM[R, (K, R)](children = ConSeq(this),
      dependency = OneToN,
      func = new ParMapAllFunc(mapAll),
      distribution = distribution,
      location = location,
      keepPartition = false,
      partitioner = new HashPartitioner(4, pFunc),
      appContext = this.appContext)
    val groupReduce = (elems: Arr[(K, R)]) => elems.toSeq.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toIterator
    new DFM[(K, R), (K, R)](children = ConSeq(parallel),
      dependency = NToOne,
      func = new ParMapAllFunc(groupReduce),
      distribution = distribution, location = location, keepPartition = true,
      partitioner = new KeepPartitioner[(K, R)](1),
      appContext = this.appContext)
  }

  def groupMapReduce[K: ClassTag, V: ClassTag](f: R => K, m: R => V, r: (V, V) => V): ParHDM[_, (K, V)] = {
    ClosureCleaner(f)
    ClosureCleaner(m)
    ClosureCleaner(r)
    val pFunc = (t: (K, V)) => t._1.hashCode()
    val mapAll = (elems: Arr[R]) => {
      elems.toSeq.groupBy(f).mapValues(_.map(m).reduce(r)).toIterator
    }
    val parallel = new DFM[R, (K, V)](children = ConSeq(this),
      dependency = OneToN,
      func = new ParMapAllFunc(mapAll),
      distribution = distribution, location = location, keepPartition = false,
      partitioner = new HashPartitioner(4, pFunc),
      appContext = this.appContext)
    val groupReduce = (elems: Arr[(K, V)]) => elems.toSeq.groupBy(e => e._1).mapValues(_.map(_._2).reduce(r)).toIterator
    new DFM(children = ConSeq(parallel),
      dependency = NToOne,
      func = new ParMapAllFunc(groupReduce),
      distribution = distribution, location = location, keepPartition = true,
      partitioner = new KeepPartitioner[(K, V)](1), appContext = this.appContext)

  }

  def count(): ParHDM[_, Int] = {
    val countFunc : Arr[R] => Arr[Int] = (elems: Arr[R]) => Arr(elems.size)
    //    val parallel = new DFM[R,Int](children = Seq(this), dependency = OneToOne, func = new ParMapAllFunc(countFunc), distribution = distribution, location = location, keepPartition = false, partitioner = new KeepPartitioner[Int](1))
    val parallel: HDM[Int] = this.mapPartitions(countFunc)
    val reduceFunc = (l1: Int, l2: Int) => l1 + l2
    new DFM[Int, Int](children = ConSeq(parallel),
      dependency = NToOne,
      func = new ParReduceFunc(reduceFunc),
      distribution = distribution,
      location = location, keepPartition = true,
      partitioner = new KeepPartitioner[Int](1),
      parallelism = 1,
      appContext = this.appContext)

  }

  def top(k: Int)(implicit ordering: Ordering[R]): ParHDM[_, R] = {
    val topFunc = (elems: Arr[R]) => elems.toSeq.sorted(ord = ordering.reverse).toIterator
    //    val parallel = new DFM[R, R](children = Seq(this), dependency = OneToOne, func = new ParMapAllFunc(topFunc), distribution = distribution, location = location, keepPartition = false, partitioner = new KeepPartitioner[R](1))
    val parallel = this.mapPartitions(topFunc)
    new DFM[R, R](children = ConSeq(parallel),
      dependency = NToOne,
      func = new ParMapAllFunc(topFunc),
      distribution = distribution, location = location, keepPartition = true,
      partitioner = new KeepPartitioner[R](1),
      parallelism = 1,
      appContext = this.appContext)

  }

  /**
    * sort the data by the ordering object
    *
    * @param preSort     whether perform pre-sorting before shuffle
    * @param ordering    the ordering object for sorting
    * @param parallelism parallelism for execution
    * @return
    */
  def sorted(preSort: Boolean)(implicit ordering: Ordering[R], parallelism: Int, hDMEntry: HDMEntry): ParHDM[_, R] = {
    val hdm = this.cache()
    val reduceNumber = parallelism * hdmContext.PLANER_PARALLEL_NETWORK_FACTOR
    val sampleSize = (math.min(100.0 * reduceNumber, 1e6) / reduceNumber).toInt
    println(s"got sample size: [${sampleSize}, reduce Number: ${reduceNumber}}].")
    val sampleFUnc = (elems: Arr[R]) => SampleUtils.randomSampling(elems.toSeq, sampleSize).toIterator
    val samples = hdm.mapPartitions(sampleFUnc).collect().toBuffer

    val bounds = RangePartitioning.decideBoundary(samples, reduceNumber)
    val partitioner = new RangePartitioner(bounds)
    if (preSort) {
      val parallel = new DFM[R, R](children = ConSeq(hdm),
        dependency = OneToOne,
        func = new SortFunc[R](false),
        distribution = distribution,
        location = location,
        keepPartition = false,
        partitioner = partitioner,
        appContext = this.appContext)

      new DFM[R, R](children = ConSeq(parallel),
        dependency = NToOne,
        func = new SortFunc[R](false),
        distribution = distribution,
        location = location,
        keepPartition = true,
        partitioner = new KeepPartitioner[R](1),
        parallelism = reduceNumber,
        appContext = this.appContext)
    } else {
      val parallel = new DFM[R, R](children = ConSeq(hdm),
        dependency = OneToOne,
        func = new NullFunc[R],
        distribution = distribution, location = location, keepPartition = false,
        partitioner = partitioner,
        appContext = this.appContext)
      new DFM[R, R](children = ConSeq(parallel),
        dependency = NToOne,
        func = new SortFunc[R](true),
        distribution = distribution, location = location, keepPartition = true,
        partitioner = new KeepPartitioner[R](1),
        parallelism = reduceNumber,
        appContext = this.appContext)
    }
  }

  /**
    * sort the data by the sorting function f
    *
    * @param f           function for deciding order of elments
    * @param preSort     whether perform pre-sorting before shuffle
    * @param parallelism parallelism for execution
    * @return
    */
  def sortBy(f: (R, R) => Int, preSort: Boolean = false)(implicit parallelism: Int, hDMEntry: HDMEntry): ParHDM[_, R] = {
    ClosureCleaner(f)
    implicit val ordering = new Ordering[R] {

      override def compare(x: R, y: R): Int = f(x, y)
    }
    sorted(preSort)
  }

  def partitionBy(func: R => Int): ParHDM[_, R] = {
    this.partition(new HashPartitioner(4, func))
    //    new DFM[R,R](children = Seq(this), dependency = OneToN, func = new NullFunc[R], distribution = distribution, location = location, keepPartition = false, partitioner = new HashPartitioner(4, func), parallelism = 1)
  }

  def partition(partitioner: Partitioner[R]): ParHDM[R, R] = {
    new DFM[R, R](children = ConSeq(this),
      dependency = OneToN,
      func = new NullFunc[R],
      distribution = distribution, location = location, keepPartition = false,
      partitioner = partitioner,
      parallelism = 1,
      appContext = this.appContext)
  }


  def compress(): HDM[R] = {
    this.isCompress = true
    this
  }

  def zipWithIndex(implicit parallelism: Int, hDMEntry: HDMEntry): HDM[(Long, R)] = {
    import scala.collection.mutable

    // get the start indices of each partition
    val lengthOfPartitions = this.mapPartitionsWithIdx { (idx, arr) =>
      Arr((idx, arr.size))
    }.collect().toMap
    val indices = mutable.HashMap.empty[Long, Long]
    indices += 0L -> 0L // start index of partition 0 set to 0
    if (lengthOfPartitions.size > 1) {
      for (idx <- 0L until lengthOfPartitions.size - 1) {
        indices += (idx + 1) -> (indices(idx) + lengthOfPartitions(idx))
      }
    }
//    log.info(s"partition index:[${indices.mkString(",")}]")

    // zip elements with global indices
    this.mapPartitionsWithIdx { (idx, arr) =>
      val localIdx = indices
      arr.zipWithIndex.map(_.swap).map(t => (t._1 + localIdx(idx), t._2))
    }
  }

  // double input functions

  def cogroup[K: ClassTag, U: ClassTag](other: HDM[U],
                                        f1: R => K,
                                        f2: U => K): HDM[(K, (Iterable[R], Iterable[U]))] = {
    val inputThis = this.partitionBy(f1(_).hashCode())
    val inputThat = other.partitionBy(f2(_).hashCode())
    val groupFunc = new CoGroupFunc[R, U, K](f1, f2)
    new DualDFM[R, U, (K, (Iterable[R], Iterable[U]))](input1 = ConSeq(inputThis),
      input2 = ConSeq(inputThat),
      dependency = NToOne,
      func = groupFunc,
      distribution = distribution,
      location = location,
      keepPartition = true,
      appContext = this.appContext)
  }

  def joinBy[K: ClassTag, U: ClassTag](other: HDM[U], f1: R => K, f2: U => K): HDM[(K, (R, U))] = {
    this.cogroup(other, f1, f2).mapPartitions { arr =>
      arr.flatMap { tup =>
        for {r <- tup._2._1; u <- tup._2._2} yield {
          (tup._1, (r, u))
        }
      }
    }
  }


  def union[A <: R](h: HDM[A]): HDM[R] = {

    new DFM[R, R](children = ConSeq(this, h.asInstanceOf[HDM[R]]),
      dependency = OneToOne,
      func = new ParUnionFunc[R],
      distribution = distribution, location = location,
      appContext = this.appContext)
  }

  def union (elems:Seq[R]): HDM[R] = {
    val nPartitions = parallelism * HDMContext.CORES
    println(s" element size ${elems.size}, min number of partitions: ${nPartitions}")
    if(nPartitions > 0  && elems.size >= nPartitions) { // shuffle to each partition
      val blocks = new RoundRobinPartitioner[R](parallelism * HDMContext.CORES ).split(elems)
      this.mapPartitionsWithIdx[R]((t1, t2) => t2 ++ blocks(t1.toInt))
    } else {
      mapPartitionsWithIdx[R]((idx, data) => if(idx == 0) data ++ elems else data) // append to the first partition
    }
  }

  def distinct[A <: R](h: HDM[A]): HDM[R] = ???

  def intersection[A <: R](h: HDM[A]): HDM[R] = ???

  // end of double input functions


  def withPartitioner(partitioner: Partitioner[R]): HDM[R] = ???

  def withParallelism(p: Int): HDM[R] = {
    this.parallelism = p
    this
  }

  def withPartitionNum(p: Int): HDM[R] = {
    if (partitioner != null) partitioner.partitionNum = p
    this
  }

  // actions

  def compute(implicit parallelism: Int, hDMEntry: HDMEntry): Future[HDM[_]] = hDMEntry.compute(this, parallelism)


  def traverse(implicit parallelism: Int, hDMEntry: HDMEntry): Future[Iterator[R]] = {
    compute(parallelism, hDMEntry).map(hdm => HDMAction.traverse[R](hdm))
  }

  def collect(maxWaiting: Long = hdmContext.JOB_DEFAULT_WAITING_TIMEOUT)(implicit parallelism: Int, hDMEntry: HDMEntry): Iterator[R] = {
    import scala.concurrent.duration._
    Await.result(traverse(parallelism, hDMEntry), maxWaiting millis)
  }


  def cached(implicit parallelism: Int, hDMEntry: HDMEntry, maxWaiting: Long = hdmContext.JOB_DEFAULT_WAITING_TIMEOUT): ParHDM[_, R] = {
    import scala.concurrent.duration._
    Await.result(compute(parallelism, hDMEntry), maxWaiting millis).asInstanceOf[ParHDM[_, R]]
  }


  def cache(): HDM[R] = {
    this.isCache = true
    this
  }


  def sample(size: Int, maxWaiting: Long)(implicit parallelism: Int, hDMEntry: HDMEntry): Iterator[R] = {
    import scala.concurrent.duration._
    Await.result(sample(Right(size)), maxWaiting millis)
  }

  def sample(percentage: Double, maxWaiting: Long)(implicit parallelism: Int, hDMEntry: HDMEntry): Iterator[R] = {
    import scala.concurrent.duration._
    Await.result(sample(Left(percentage)), maxWaiting millis)
  }

  def sample(proportion: Either[Double, Int])(implicit parallelism: Int, hDMEntry: HDMEntry): Future[Iterator[R]] = {
    proportion match {
      case Left(percentage) =>
        val sampleFunc = (in: Arr[R]) => {
          val size = (percentage * in.size).toInt
          Random.shuffle(in).take(size)
        }
        sample(sampleFunc)
      case Right(size) =>
        val sizePerPartition = Math.max(1, Math.round(size.toFloat / (parallelism)))
        val sampleFunc = (in: Arr[R]) => {
          in.take(sizePerPartition)
        }
        sample(sampleFunc).map { arr => arr.take(size) }
    }
  }

  def sample(sampleFunc: Arr[R] => Arr[R])(implicit parallelism: Int, hDMEntry: HDMEntry): Future[Iterator[R]] = {
    this.mapPartitions(sampleFunc).traverse(parallelism, hDMEntry)
  }

  def toSerializable(): HDM[R]

  // end of actions

  //  def copy(id: String = this.id,
  //           children:Seq[AbstractHDM[_]]= this.children,
  //           dependency: DataDependency = this.dependency,
  //           func: SerializableFunction[_, Arr[R]] = this.func,
  //           blocks: Seq[String] = null,
  //           distribution: Distribution = this.distribution,
  //           location: Path = this.location,
  //           preferLocation:Path = this.preferLocation,
  //           blockSize:Long = this.blockSize,
  //           isCache: Boolean = this.isCache,
  //           state: BlockState = this.state,
  //           parallelism: Int = this.parallelism,
  //           keepPartition: Boolean = this.keepPartition,
  //           partitioner: Partitioner[R] = this.partitioner):AbstractHDM[R]

}


/**
  * HDM data factory
  */
object HDM {

  def apply[T: ClassTag](elems: Array[T], appContext: AppContext): DDM[_, T] = {
    DDM(elems, HDMContext.defaultContext, appContext)
  }

  def apply(path: Path,
            appContext: AppContext = AppContext.defaultAppContext,
            hdmContext: HDMContext = HDMContext.defaultContext,
            keepPartition: Boolean = true): DFM[_, String] = {
    new DFM(children = null,
      location = path,
      keepPartition = keepPartition,
      func = new NullFunc[String],
      appContext = appContext)
  }


  def parallelize[T: ClassTag](elems: Seq[T],
                               hdmContext: HDMContext = HDMContext.defaultContext,
                               appContext: AppContext = AppContext.defaultAppContext,
                               numOfPartitions: Int = HDMContext.CORES): HDM[T] = {
    require(elems.length >= numOfPartitions)
    val ddms = new RoundRobinPartitioner[T](numOfPartitions).split(elems).map(d => DDM(d._2, hdmContext, appContext))
    new DFM(children = ddms.toSeq,
      func = new NullFunc[T],
      distribution = Horizontal,
      location = Path(hdmContext.clusterBlockPath),
      appContext = appContext)
  }


  def parallelWithIndex[T: ClassTag](elems: Seq[T],
                                     hdmContext: HDMContext = HDMContext.defaultContext,
                                     appContext: AppContext = AppContext.defaultAppContext,
                                     numOfPartitions: Int = HDMContext.CORES): HDM[(Long, T)] = {
    require(elems.length >= numOfPartitions)
    val ddms = new RoundRobinPartitioner[(Long, T)](numOfPartitions)
      .split(elems.zipWithIndex.map(_.swap).map(tup => (tup._1.toLong, tup._2)))
      .map(d => DDM(d._2, hdmContext, appContext))
    new DFM(children = new ConSeq(ddms.toSeq),
      func = new NullFunc[(Long, T)],
      distribution = Horizontal,
      location = Path(hdmContext.clusterBlockPath),
      appContext = appContext)

  }

  def jParallelize[T](elems: Array[T],
                      hdmContext: HDMContext = HDMContext.defaultContext,
                      appContext: AppContext = AppContext.defaultAppContext,
                      numOfPartitions: Int = HDMContext.CORES): HDM[T] = {
    require(elems.length >= numOfPartitions)
    val c = elems.head.getClass
    val ct = ClassTag.apply(c)
    parallelize[ct.type](elems.toSeq.asInstanceOf[Seq[ct.type]],
      hdmContext,
      appContext,
      numOfPartitions).asInstanceOf[HDM[T]]
  }

  def fromURL[T:ClassTag](urls: Array[Path],
                          appContext: AppContext = AppContext.defaultAppContext,
                          hdmContext: HDMContext = HDMContext.defaultContext,
                          func: Arr[Byte] => Arr[T],
                          keepPartition: Boolean = true): HDM[T] = {

    val inputs = DDM.sources[Byte](urls.toSeq, hdmContext, appContext)

    new DFM(children = inputs,
      func = new ParUnionFunc[Byte],
      parallelism = 1,
      location = Path(hdmContext.clusterBlockPath),
      appContext = appContext).mapPartitions(func(_))
  }

  def horizontal[T: ClassTag](appContext: AppContext, hdmContext: HDMContext, elems: Array[T]*): HDM[T] = {
    val inputs = elems.map(e => DDM(e, hdmContext, appContext))
    new DFM(children = inputs,
      func = new ParUnionFunc[T],
      distribution = Horizontal,
      parallelism = 1,
      location = Path(hdmContext.clusterBlockPath),
      appContext = appContext)
  }

  def vertical[T: ClassTag](paths: Array[Path], func: String => T): HDM[T] = ???



}

