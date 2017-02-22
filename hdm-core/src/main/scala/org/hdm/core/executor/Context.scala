package org.hdm.core.executor

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.typesafe.config.{Config, ConfigFactory}
import org.hdm.akka.server.SmsSystem
import org.hdm.core.coordinator.HDMWorkerParams
import org.hdm.core.functions.SerializableFunction
import org.hdm.core.io.netty.NettyConnectionManager
import org.hdm.core.io.{CompressionCodec, SnappyCompressionCodec}
import org.hdm.core.message._
import org.hdm.core.model.{GroupedSeqHDM, HDM, KvHDM, ParHDM}
import org.hdm.core.planing.{StaticMultiClusterPlanner, StaticPlaner}
import org.hdm.core.scheduling.{AdvancedScheduler, MultiClusterScheduler, SchedulingPolicy}
import org.hdm.core.serializer.{SerializerInstance, KryoSerializer, JavaSerializer}
import org.hdm.core.server._
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.hdm.core.utils.Logging

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Tiantian on 2014/11/4.
 */
trait Context {

  def findHDM(id: String): ParHDM[_, _] = ???

  def sendFunc[T, R](target: ParHDM[_, T], func: SerializableFunction[T, R]): Future[ParHDM[T, R]] = ???

  def receiveFunc[T, R](target: ParHDM[_, T], func: SerializableFunction[T, R]): Future[ParHDM[T, R]] = ???

  def runTask[T, R](target: ParHDM[_, T], func: SerializableFunction[T, R]): Future[ParHDM[T, R]] = ???
}

class HDMContext(defaultConf: Config) extends Serializable with Logging {

  lazy val PLANER_PARALLEL_CPU_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.cpu.factor")
  } getOrElse (HDMContext.CORES)

  lazy val PLANER_PARALLEL_NETWORK_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.network.factor")
  } getOrElse (HDMContext.CORES)

  val PLANER_is_GROUP_INPUT = Try {
    defaultConf.getBoolean("hdm.planner.input.group")
  } getOrElse (false)

  val PLANER_INPUT_GROUPING_POLICY = Try {
    defaultConf.getString("hdm.planner.input.group-policy")
  } getOrElse ("weighted")

  val BLOCK_COMPRESS_IN_TRANSPORTATION = Try {
    defaultConf.getBoolean("hdm.io.network.block.compress")
  } getOrElse (true)

  val NETTY_BLOCK_SERVER_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.server.threads")
  } getOrElse (HDMContext.CORES)

  val NETTY_BLOCK_CLIENT_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.client.threads")
  } getOrElse (HDMContext.CORES)

  val NETTY_CLIENT_CONNECTIONS_PER_PEER = Try {
    defaultConf.getInt("hdm.io.netty.client.connection-per-peer")
  } getOrElse (HDMContext.CORES)

  val SCHEDULING_FACTOR_CPU = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.cpu")
  } getOrElse (1)

  val SCHEDULING_FACTOR_IO = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.io")
  } getOrElse (10)

  val SCHEDULING_FACTOR_NETWORK = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.network")
  } getOrElse (20)

  val BLOCK_SERVER_INIT = Try {
    defaultConf.getBoolean("hdm.io.netty.server.init")
  } getOrElse (true)

  val BLOCK_SERVER_PROTOCOL = Try {
    defaultConf.getString("hdm.io.network.protocol")
  } getOrElse ("netty")

  var NETTY_BLOCK_SERVER_PORT = Try {
    defaultConf.getInt("hdm.io.netty.server.port")
  } getOrElse (9091)

  val SCHEDULING_POLICY_CLASS = Try {
    defaultConf.getString("hdm.scheduling.policy.class")
  } getOrElse ("org.hdm.core.scheduling.MinMinScheduling")

  lazy val DEFAULT_DEPENDENCY_BASE_PATH = Try {
    defaultConf.getString("hdm.dep.base.path")
  } getOrElse ("target/repo/hdm")

  lazy val parallelismFactor = Try {
    defaultConf.getDouble("hdm.executor.parallelism.factor")
  } getOrElse (1.0D)

  lazy val mockExecution = Try {
    defaultConf.getBoolean("hdm.executor.mockExecution")
  } getOrElse (false)

  val MAX_MEM_GC_SIZE = Try {
    defaultConf.getInt("hdm.memory.gc.max.byte")
  } getOrElse (1024 * 1024 * 1024) // about 256MB

  lazy val DEFAULT_BLOCK_ID_LENGTH = defaultSerializer.serialize(newLocalId()).array().length

  val defaultSerializer: SerializerInstance = new JavaSerializer(defaultConf).newInstance()

  val defaultSerializerFactory = new JavaSerializer(defaultConf)

//  val jobSerializerFactory = new KryoSerializer(defaultConf)

//  val jobSerializer: SerializerInstance = new KryoSerializer(defaultConf).newInstance()

  val compressor = HDMContext.DEFAULT_COMPRESSOR

  val cores = HDMContext.CORES

  implicit lazy val executionContext = ClusterExecutorContext((cores * parallelismFactor).toInt)

  val slot = new AtomicInteger(1)

  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  val planer = new StaticPlaner(this)


  private var hdmBackEnd: ServerBackend = null

  lazy val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]

  //  val scheduler = new SimpleFIFOScheduler

  val leaderPath: AtomicReference[String] = new AtomicReference[String]()

  def blockContext() = {
    BlockContext(leaderPath.get() + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
  }


  def startAsMaster(port: Int = 8999, conf: Config = defaultConf, slots: Int = 0, mode: String = "single-cluster") {
    SmsSystem.startAsMaster(port, isLinux, conf)
    //    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.ClusterExecutorLeader", slots)
    //    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.HDMClusterLeaderActor", slots)
    val masterCls = if (mode == "multi-cluster") "org.hdm.core.coordinator.HDMMultiClusterLeader"
    else "org.hdm.core.coordinator.HDMClusterLeaderActor"
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost", masterCls, slots)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    leaderPath.set(SmsSystem.physicalRootPath)
  }

  def startAsSlave(masterPath: String, port: Int = 10010, blockPort: Int = 9091, conf: Config = defaultConf, slots: Int = cores) {
    this.slot.set(slots)
    this.NETTY_BLOCK_SERVER_PORT = blockPort
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
      "org.hdm.core.coordinator.HDMClusterWorkerActor",
      HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, slots, blockContext))
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost",
      "org.hdm.core.coordinator.BlockManagerFollower",
      masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost",
      "org.hdm.core.coordinator.ResultHandler", null)
    leaderPath.set(masterPath)
    if (BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(this)
  }

  def startAsClient(masterPath: String, port: Int = 20010, blockPort: Int = 9092, conf: Config = defaultConf, localExecution: Boolean = false) {
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerFollower", masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
    leaderPath.set(masterPath)
    this.NETTY_BLOCK_SERVER_PORT = blockPort
    if (BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(this)
    if (localExecution) {
      this.slot.set(cores)
      SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
        "org.hdm.core.coordinator.HDMClusterWorkerActor",
        HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, cores, blockContext))
    }

  }

  def init(leader: String = "localhost", slots: Int = cores) {

    if (leader == "localhost") {
      startAsMaster(slots = slots)
    } else {
      if (slots > 0)
        startAsClient(masterPath = leader, localExecution = true)
      else
        startAsClient(masterPath = leader, localExecution = false)
    }
    //    scheduler.start()
  }


  def shutdown() {
    SmsSystem.shutDown()
  }


  def getServerBackend(mode: String = "single"): ServerBackend = {
    if (hdmBackEnd == null) mode match {
      case "single" =>
        val appManager = new AppManager
        val blockManager = HDMBlockManager()
        val promiseManager = new DefPromiseManager
        val resourceManager = new SingleClusterResourceManager
        val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        //    val scheduler = new DefScheduler(blockManager, promiseManager, resourceManager, SmsSystem.system)
        val scheduler = new AdvancedScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, schedulingPolicy)
        hdmBackEnd = new HDMServerBackend(blockManager, scheduler, planer, resourceManager, promiseManager, DependencyManager(), this)
        log.info(s"created new HDMServerBackend with scheduling: ${SCHEDULING_POLICY_CLASS}")

      case "multiple" =>
        val appManager = new AppManager
        val blockManager = HDMBlockManager()
        val promiseManager = new DefPromiseManager
        val resourceManager = new MultiClusterResourceManager
        val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        val multiPlanner = new StaticMultiClusterPlanner(planer, HDMContext.defaultHDMContext)
        val scheduler = new MultiClusterScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, DependencyManager(), multiPlanner, schedulingPolicy, this)
        hdmBackEnd = new MultiClusterBackend(blockManager, scheduler, multiPlanner, resourceManager, promiseManager, DependencyManager(), this)
        log.info(s"created new MultiClusterBackend with scheduling: ${SCHEDULING_POLICY_CLASS}")
    }
    hdmBackEnd
  }

  def submitJob(master: String, appName: String, version: String, hdm: HDM[_], parallel: Int): Future[HDM[_]] = {
    val rootPath = SmsSystem.physicalRootPath
    //    HDMContext.declareHdm(Seq(hdm))
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER, RegisterPromiseMsg(appName, version, hdm.id, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[HDM[_]]]
      case none => null
    }
    //    val jobMsg = SubmitJobMsg(appId, hdm, rootPath + "/"+JOB_RESULT_DISPATCHER, parallel)
    val jobBytes = HDMContext.JOB_SERIALIZER.serialize(hdm).array
    val jobMsg = new SerializedJobMsg(appName, version, jobBytes, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER, rootPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, parallel)
    SmsSystem.askAsync(master + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, jobMsg)
    if (promise ne null) promise.future
    else throw new Exception("add job dispatcher failed.")
  }


  def explain(hdm: HDM[_], parallelism: Int) = {
    //    val hdmOpt = new FunctionFusion().optimize(hdm)
    //    planer.plan(hdmOpt, parallelism)
    planer.plan(hdm, parallelism)
  }

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]] = {
    //    addJob(hdm.id, explain(hdm, parallelism))
    submitJob(hdm.appContext.masterPath,
      hdm.appContext.appName,
      hdm.appContext.version,
      hdm, parallelism)
  }

  def declareHdm(hdms: Seq[ParHDM[_, _]], declare: Boolean = true) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddRefMsg(hdms, declare))
  }

  def addBlock(block: Block[_], declare: Boolean) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddBlockMsg(block, declare))
  }

  def queryBlock(id: String, location: String) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, QueryBlockMsg(Seq(id), location))
  }

  def removeBlock(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveBlockMsg(id))
  }

  def removeRef(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveRefMsg(id))
  }

  def addTask(task: Task[_, _]) = {
    SmsSystem.askAsync(leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddTaskMsg(task))
  }

  @Deprecated
  def submitTasks(appId: String, hdms: Seq[ParHDM[_, _]]): Future[ParHDM[_, _]] = {
    val rootPath = SmsSystem.rootPath
    this.declareHdm(hdms)
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[ParHDM[_, _]]]
      case none => null
    }
    SmsSystem.askAsync(leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER))

    if (promise ne null) promise.future
    else throw new Exception("add job dispatcher failed.")
  }


  def clean(appId: String): Unit = {
    //todo clean all the resources used by this application
  }

  def clusterBlockPath = {
    leaderPath.get() + "/" + HDMContext.BLOCK_MANAGER_NAME
  }

  def localBlockPath = {
    BLOCK_SERVER_PROTOCOL match {
      case "akka" => SmsSystem.physicalRootPath + "/" + HDMContext.BLOCK_MANAGER_NAME
      case "netty" => s"netty://${NettyConnectionManager.localHost}:${NETTY_BLOCK_SERVER_PORT}"
    }
  }


  def getCompressor(): CompressionCodec = {
    if (BLOCK_COMPRESS_IN_TRANSPORTATION) compressor
    else null
  }


  def newClusterId(): String = {
    UUID.randomUUID().toString
  }

  def newLocalId(): String = {
    UUID.randomUUID().toString
  }


}


object HDMContext extends Logging {

  val _defaultConf = new AtomicReference[Config]()

  private val jobSerializer = new ThreadLocal[SerializerInstance]()

  lazy val defaultHDMContext = apply()

  val CLUSTER_EXECUTOR_NAME: String = "ClusterExecutor"

  val BLOCK_MANAGER_NAME: String = "BlockManager"

  val JOB_RESULT_DISPATCHER: String = "ResultDispatcher"

  val DEFAULT_SERIALIZER: SerializerInstance = new JavaSerializer(defaultConf).newInstance()

  val DEFAULT_SERIALIZER_FACTORY = new JavaSerializer(defaultConf)

  def JOB_SERIALIZER: SerializerInstance = {
    if(jobSerializer.get() == null){
      jobSerializer.set(new KryoSerializer(defaultConf).newInstance())
    }
    jobSerializer.get()
  }

  def reNewJobSerilizer(ser:SerializerInstance): Unit ={
    jobSerializer.set(ser)
  }

  val DEFAULT_COMPRESSOR = new SnappyCompressionCodec(defaultConf)

  lazy val DEFAULT_BLOCK_ID_LENGTH = DEFAULT_SERIALIZER.serialize(newLocalId()).array().length

  val CORES = Runtime.getRuntime.availableProcessors

  def defaultConf() = _defaultConf.get()

  def setDefaultConf(conf: Config) = _defaultConf.set(conf)

  def newClusterId(): String = {
    UUID.randomUUID().toString
  }

  def newLocalId(): String = {
    UUID.randomUUID().toString
  }

  def apply() = {
    if (defaultConf == null) {
      setDefaultConf(ConfigFactory.load("hdm-core.conf"))
    }
    new HDMContext(defaultConf)
  }

  def apply(conf: Config) = {
    setDefaultConf(conf)
    new HDMContext(conf)
  }

  def declareHdm(hdms: Seq[ParHDM[_, _]], declare: Boolean = true) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddRefMsg(hdms, declare))
  }

  def addBlock(block: Block[_], declare: Boolean) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddBlockMsg(block, declare))
  }

  def queryBlock(id: String, location: String) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, QueryBlockMsg(Seq(id), location))
  }

  def removeBlock(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveBlockMsg(id))
  }

  def removeRef(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveRefMsg(id))
  }

  /**
   *
   * @param hdm
   * @tparam K
   * @tparam V
   * @return
   */
  implicit def hdmToKVHDM[K: ClassTag, V: ClassTag](hdm: HDM[(K, V)]): KvHDM[K, V] = {
    new KvHDM(hdm)
  }

  implicit def hdmToGroupedSeqHDM[K: ClassTag, V: ClassTag](hdm: ParHDM[_, (K, Iterable[V])]): GroupedSeqHDM[K, V] = {
    new GroupedSeqHDM[K, V](hdm)
  }
}