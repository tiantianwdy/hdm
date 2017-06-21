package org.hdm.akka.server

import concurrent.duration.{ Duration, DurationInt }
import org.hdm.akka.actors.MasterActor
import org.hdm.akka.actors.SlaveActor
import org.hdm.akka.actors.worker.PersistenceActor
import org.hdm.akka.configuration.Parameters
import org.hdm.akka.messages._
import org.hdm.akka.persistence.{ PersistenceExtensionProvider, PersistenceService, CachedKeyValueService }
import com.typesafe.config.{ ConfigFactory, Config }
import akka.AkkaException
import akka.actor._
import akka.actor.ActorSelection.toScala
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import org.hdm.akka.extensions.{RemoteAddressExtension, RoutingExtension, ActorConfigProvider, RoutingExtensionProvider}
import org.hdm.akka.configuration.ActorConfig
import akka.remote.RemoteScope
import org.hdm.akka.configuration.ActorConfig
import concurrent.{ Await, Future }
import org.hdm.akka.messages.AddMsg
import org.hdm.akka.messages.UpdateMsg
import akka.remote.RemoteScope
import org.hdm.akka.messages.JoinMsg
import org.hdm.akka.messages.InitMsg
import org.hdm.akka.configuration.ActorConfig
import org.hdm.akka.messages.StopMsg
import java.util.concurrent.TimeUnit
import java.net.InetAddress

import scala.util.Random

/**
 * Akka system 的接口封装类，可以直接使用
 * 具体使用示例参考 SmsSystemTest
 * @author wudongyao
 * @version since 0.0.1
 *
 */
object SmsSystem {

  /**
   * 接收回调响应的超时时间
   */
  implicit val timeout = Timeout(600 seconds)
  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)

  val MASTER_SYS_NAME = "masterSys"
  val SLAVE_SYS_NAME = "slaveSys"
  val MASTER_NAME = "smsMaster"
  val SLAVE_NAME = "smsSlave"
  val HEARTBEAT_ACTOR_NAME = "heartbeatActor"
  val SLAVE_BUSS_PATH = s"$SLAVE_SYS_NAME/user/$SLAVE_NAME"
  val MASTER_BUSS_PATH = s"$MASTER_SYS_NAME/user/$MASTER_NAME"

  /**
   *
   */
  var IS_LINUX: Boolean = false
  /**
   * 用于创建actor的ActorSystem，在startAsSlave或者startAsMaster后被初始化
   * 不直接初始化的原因是ActorSystem一旦初始化就会占用一个端口
   */
  var system: ActorSystem = null

  var systemPort: Option[Int] = None

  lazy val systemHost: String = RoutingExtension.hostAddress

  /**
   * actorSystem的默认消息存储实现，暂时没有使用,待完善
   * 测试时用来临时存储监控数据
   */
  var persistenceService: PersistenceService = new CachedKeyValueService(256)


  /**
    * rootActor 所有的新actor会在这个根Actor之下创建
   * 对于master：这个Actor负责和所有给Slave的 rootActor进行通信和管理
   * 对于slave: 这个actor负责和master通信，并管理和创建子Actor
   * 对于client: 这个actor记录远程master的地址，负责进行远程通信
   * 这个actor 在actorSystem启动后会初始化
   *
   */
  private var rootActor: ActorSelection = null

  /**
    * actor reference of rootActor
   * 对于client 这个字段为 null
   * 为了代码兼容性保留，不推荐直接使用，可以用@rootActor代替
   */
  private var rootActorRef: ActorRef = null


  def loadConf(defaultConf: Config) = if (defaultConf == null) {
    ConfigFactory.load()
  } else defaultConf

  /**
   * 将这个actor system初始化为Master
    * @param host starting host
    * @param port 启动端口
    * @param isLinux 是否是Linux操作系统，影响资源监控策略的实现
    * @param conf 启动配置，如果为空则从classpath下读取配置，非空则使用这个配置，包括覆盖port参数
   */
  def startAsMaster(host:String, port: Int, isLinux: Boolean, conf: Config = null) {
    //    if (system ne null) throw new AkkaException("Already started a  local actor system..")
    val hostname = if(host eq null) "" else host
    val config = if (port > 0) {
      ConfigFactory.parseString(s"""
      akka.remote {
          netty.tcp {
            hostname = "${hostname}"
            port = $port
          }
        }
        """).withFallback(loadConf(conf))
    }
    else loadConf(conf)
    systemPort = Option(config.getInt("akka.remote.netty.tcp.port"))
    IS_LINUX = isLinux
    system = ActorSystem(MASTER_SYS_NAME, config)
    persistenceService = PersistenceExtensionProvider(system)
    val persistenceActor = system.actorOf(Props(new PersistenceActor(persistenceService)), name = "persistenceActor")
    rootActorRef = system.actorOf(Props(new MasterActor(persistenceActor)), MASTER_NAME)
    rootActor = ActorSelection(rootActorRef, rootActorRef.path.toString)
    rootActor ! InitMsg(null)
  }

  /**
   * 将这个actor system初始化为slave
   * @param port 启动端口
   * @param isLinux 是否是Linux操作系统
   * @param conf 启动配置，如果为空则从classpath下读取配置
   */
  def startAsSlave(masterPath: String, port: Int, isLinux: Boolean, conf: Config = null) = {
    //    if (system ne null) throw new AkkaException("Already started a  local actor system..")
    //        shutDown(1000, 5000)
    val config = if (port > 0) ConfigFactory.parseString(s"""akka.remote.netty.tcp.port="${port}" """).withFallback(loadConf(conf))
    else loadConf(conf)
    systemPort = Option(config.getInt("akka.remote.netty.tcp.port"))
    IS_LINUX = isLinux
    system = ActorSystem(SLAVE_SYS_NAME, config)
//    persistenceService = PersistenceExtensionProvider(system)
    rootActorRef = system.actorOf(Props(new SlaveActor(masterPath)), SLAVE_NAME)
    rootActor = ActorSelection(rootActorRef, rootActorRef.path.toString)
    rootActorRef.path.toString
  }

  /**
   * 作为client 启动，只负责与master进行通信
   * @param masterPath master Actor地址
   * @param port 本地actor system端口
   * @param conf 本地配置
   * @return master Actor地址
   */
  def startAsClient(masterPath: String, port: Int, conf: Config = null) = {
    val config = if (conf eq null) {
      System.setProperty("akka.remote.netty.tcp.port", port.toString)
      ConfigFactory.load()
    } else conf
    system = ActorSystem(SLAVE_SYS_NAME, config)
    systemPort = Option(config.getInt("akka.remote.netty.tcp.port"))
    rootActor = system.actorSelection(masterPath)
    rootActor.toString()
  }

  /**
   * 向actor发送消息
   * @param actorPath 消息发送的actor地址
   * @param message 消息对象
   */
  def forwardMsg(actorPath: String, message: Any) {
    if (system eq null) {
      val conf = ConfigFactory.load()
      ActorSystem("default", conf).actorSelection(actorPath) ! message
    } else system.actorSelection(actorPath) ! message
  }

  /**
   * 向actor发送一个消息并等待响应,以future对象返回
   * @param actorPath 消息发送的actor地址
   * @param msg 消息对象
   * @return Option[Result]
   */
  def askSync(actorPath: String, msg: Any) = {
    val future = askAsync(actorPath, msg)
    try {
      Await.result[Any](future, maxWaitResponseTime) match {
        case result: Option[Any] => result
        case result: Any if (result != null) => Some(result)
        case _ => None
      }
    } catch {
      case e: Throwable => System.err.println(e); None
    }
  }

  /**
   *
   * @param actorPath
   * @param msg
   * @return Future[Any]
   */
  def askAsync(actorPath: String, msg: Any) = {
    if ((system eq null) || system.isTerminated) {
      val port:Int = (15001 + Random.nextFloat()*5000).toInt
      val conf = ConfigFactory.parseString(s"""akka.remote.netty.tcp.port = "$port" """)
        .withFallback(ConfigFactory.load())
      system = ActorSystem("default", conf)
    }
    ask(system.actorSelection(actorPath), msg)
  }

  /**
   * 向本地ActorSystem中的actor发送消息，默认会使用本地rootPath下的actor
   * @param actorName actor名字，用于拼接actorPath, path=${rootPath}/${actorName}
   * @param msg 消息对象
   */
  def forwardLocalMsg(actorName: String, msg: Any) {
    if (system eq null) {
      throw new AkkaException("Uninitiated local actor system")
    }
    system.actorSelection(s"$rootPath/$actorName") ! msg
  }

  /**
   * 向本地ActorSystem中的actor发送消息，并等待响应,以future对象返回, 默认会使用本地rootPath下的actor
   * @param actorName actor名字，用于拼接actorPath, path=${rootPath}/${actorName}
   * @param msg 消息对象
   * @return future
   */
  def askLocalMsg(actorName: String, msg: Any) = {
    if (system eq null) {
      throw new AkkaException("Uninitiated local actor system")
    }
    askSync(s"$rootPath/$actorName", msg)
  }

  /**
   * 通过向rootActor发送一条AddMsg来创建一个Actor
   * master 收到消息后会根据msg.slavePath 来查找slave节点后转发这条消息
   * slave 收到消息后会在rootPath下创建一个满足配置的Actor并把 msg.parameter作为初始化参数传给它，完成初始化
   * @param msg  参考 [[org.hdm.akka.messages.AddMsg]]
   */
  def addActor(msg: AddMsg) = {
    if ((system ne null) && !system.isTerminated && (rootActor ne null)) {
      system.actorSelection(rootPath)! msg
      s"${msg.path}/${msg.name}"
    } else throw new AkkaException("Uninitiated local actor system")
  }

  /**
   * 通过参数直接创建一个Actor
   * 底层调用的是 [[org.hdm.akka.server.SmsSystem.addActor( ) ]]
   * master 会根据slavePath 来查找slave节点后转发这条消息
   * slave 收到消息后会在rootPath下创建一个满足配置的Actor并把 msg.parameter作为初始化参数传给它，完成初始化
   * @param name actorName
   * @param slavePath slave的rootActor地址，新的Actor会被创建在这个地址下
   * @param clazzName 创建Actor的实现类
   * @param params 初始化Actor的参数，任意类型，但是需要被构造的Actor具有对应类型的构造函数
   */
  def addActor(name: String, slavePath: String, clazzName: String, params: Any): String = {
    addActor(AddMsg(name, slavePath, clazzName, params))
  }

  /**
   * 直接通过本地actorSystem在远程节点上创建一个Actor，要求远程地址上已有一个启动的ActorSystem
   * 创建的新actor， path=akka.tcp://${remotePath}/remote/${localPath}/actorName
   * 新创建的actor会和本地Actor通过长连接保持联系，如果远程ActorSystem关闭则会报错
   * 因为这个创建过程是同步的并且actorPath自动产生在远程系统根路径下，不推荐使用
   */
  def addRemoteActor(name: String, path: String, actorClazz: Actor, config: Parameters) = {
    if (system eq null) {
      throw new AkkaException("Uninitiated local actor system")
    }
    val address = AddressFromURIString(path)
    val actorRef = system.actorOf(Props(actorClazz).withDeploy(Deploy(scope = RemoteScope(address))), name)
    actorRef.path.toString
    //    managerMap(actorClazz.getClass().toString()).addActorInstance(config, actorRef.path.toString)
  }

  /**
   * 更新一个actor对应的初始化参数
   * @param actorPath  actor的绝对路径
   * @param params  actor参数
   */
  def updateConfig(actorPath: String, confId: String, params: Any, propagate: Boolean) {
    rootActor ! UpdateMsg(actorPath, confId, params, discardOld = false, propagate = propagate)
  }

  def getParam(actorPath: String): Any = {
    val configExtension = ActorConfigProvider(system)
    configExtension.getConf(actorPath) match {
      case conf: ActorConfig => conf.params
      case _ => null
    }
  }

  /**
   * 用于向心跳actor注册需要监控的actor，被监控的Actor需要能够处理CollectMsg
   * 心跳actor目前只对slaveActor有效
   *
   */
  def registerMonitor(actor: ActorRef) {
    if (system eq null) throw new AkkaException("Uninitiated local actor system")
    val hbPath = system / (SLAVE_NAME) / (HEARTBEAT_ACTOR_NAME)
    system.actorSelection(hbPath) ! JoinMsg(slavePath = actor.path.toString)
  }

  /**
   *
   * @param parent
   * @param name
   */
  def removeActor(parent: String, name: String) {
    rootActor ! StopMsg(parent, name)
  }

  /**
   * 同步关闭这个ActorSystem，会等待直到成功关闭
   */
  def shutDown(interval: Long, maxWaitTime: Long) {
    if (system ne null) {
      var waitTime: Long = 0
      system.shutdown()
      while (!system.isTerminated && waitTime < maxWaitTime) {
        Thread.sleep(interval)
        waitTime += interval
      }
      if (waitTime > maxWaitTime)
        System.err.println(s"ActorSystem shutdown time-out over:  $maxWaitTime ms.")
    }
  }

  /**
   * 关闭ActorSystem,但是不会等待
   */
  def shutDown() {
    if (system ne null) {
      system.shutdown()
      system.awaitTermination()
    }
  }

  /**
   * 返回rootActor的Path
   */
  def rootPath = if (rootActorRef ne null) rootActorRef.path.toString
  else throw new AkkaException("Uninitiated local actor system")

  def localAddress = RemoteAddressExtension(system).address

  def physicalRootPath = {
    if (rootActorRef ne null) {
      rootActorRef.path.toStringWithAddress(localAddress)
    }
    else throw new AkkaException("Uninitiated local actor system")
  }

  // monitor related methods

  def listAllRooting() = {
    if (system eq null) throw new AkkaException("Uninitiated local actor system")
    val rootingService = RoutingExtensionProvider(system)
    rootingService.getAllRooting
  }

  def allSlaves() = {
    if (system eq null) throw new AkkaException("Uninitiated local actor system")
    val rootingService = RoutingExtensionProvider(system)
    rootingService.getRooting(SLAVE_BUSS_PATH)
  }

  def children(slavePath: String) = {
    val rootingService = RoutingExtensionProvider(system)
    rootingService.find(_.deploy.parentPath == slavePath)
  }

}