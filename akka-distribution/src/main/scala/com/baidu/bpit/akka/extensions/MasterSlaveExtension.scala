package com.baidu.bpit.akka.extensions

import com.baidu.bpit.akka.messages._
import akka.actor._
import akka.pattern.ask
import collection.mutable
import concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.baidu.bpit.akka.actors.HeartBeatActor
import com.baidu.bpit.akka.server.SmsSystem
import com.baidu.bpit.akka.configuration.Deployment
import akka.util.Timeout
import com.baidu.bpit.akka._
import configuration.ActorConfig
import messages.AddMsg
import messages.HeartbeatMsg
import messages.InitMsg
import messages.JoinMsg
import messages.LeftMsg
import messages.RoutingAddMsg
import messages.RoutingRemove
import messages.RoutingSyncMsg
import messages.RoutingUpdateMsg
import messages.SlaveParams
import messages.StateMsg
import messages.StopMsg
import messages.SuspendMsg
import messages.UpdateMsg
import scala.Some
import akka.remote.RemoteScope

/**
 * master-slave通信协议实现
 *
 * @author wudongyao
 * @date 13-8-3 ,下午2:21
 * @version 0.0.1
 */
trait MasterSlaveExtension {
  /**
   * master-slave 架构中的节点类型
   */
  val actorType: MasterSlaveActorType

  /**
   * 基本的命名常量
   */
  val MASTER_SYS_NAME = "masterSys"
  val SLAVE_SYS_NAME = "slaveSys"
  val MASTER_NAME = "smsMaster"
  val SLAVE_NAME = "smsSlave"
  val HEARTBEAT_ACTOR_NAME = "heartbeatActor"
  /**
   * slave root节点的逻辑路径
   */
  val SLAVE_BUSS_PATH = s"$SLAVE_SYS_NAME/user/$SLAVE_NAME"

  /**
   * Master-slave消息处理接口
   * @param msg 消息体
   */
  def handleMasterSlaveMsg(msg: MasterSlaveProtocol)
}

/**
 * Master slave 的actor扩展，实现了基本的消息分发逻辑，具体操作根据子类节点类型实现
 */
trait MasterSlaveActorExtension extends MasterSlaveExtension with ActorLogging {
  this: Actor =>

  val actorType: MasterSlaveActorType

  lazy val globalActorAddress = self.path.toStringWithAddress(RoutingExtension.globalSystemAddress())

  def handleMasterSlaveMsg(msg: MasterSlaveProtocol) {
    msg match {
      case hbMsg: HeartbeatMsg => heartBeat(sender, hbMsg)
      case JoinMsg(mPath, slavePath) => join(mPath, slavePath)
      case InitMsg(params) => init(params)
      case StateMsg(state, cause) => stateChanged(state, cause)
      case AddMsg(name, path, clazz, conf) => sender ! add(name, path, clazz, conf)
      case LeftMsg(slavePath, _) => left(slavePath)
      case UpdateMsg(path, configId, params, disregardOld, propagate) => update(path, configId, params, disregardOld, propagate)
      case StopMsg(parent, name) => stop(parent, name)
      case _ => log.warning("unhandled msg:" + msg + "from " + sender.path)
    }
  }

  def init(params: Any): Int

  def stateChanged(state: Int, cause: Any)

  def add(actorId: String, slavePath: String, clazz: String, parameters: Any): Int

  def join(masterPath: String, actorPath: String)

  def left(actorPath: String)

  def heartBeat(actor: ActorRef, msg: HeartbeatMsg)

  def update(path: String, configId: String, param: Any, discardOld: Boolean, propagate: Boolean)

  def stop(parent: String, name: String)

}

/**
 * master-slave协议中master端消息处理实现
 */
trait MasterExtensionImpl extends MasterSlaveActorExtension with RoutingService with HeartbeatWatcher {

  this: Actor =>

  import scala.collection.mutable
  import scala.concurrent.Await
  import scala.concurrent.duration.DurationInt

  implicit val timeout = Timeout(60 seconds)

  val actorType = Master

  var persistenceActor: ActorRef

  val cancellableList: mutable.ListBuffer[Cancellable] = mutable.ListBuffer()

  /**
   * 使用当前actorSystem中的ActorConfigExtension
   */
  private val configExtension = ActorConfigProvider(context.system)

  /**
   * master 初始化逻辑
   * @param params 初始化参数
   * @return  state : Int 初始化状态，1标识成功
   */
  def init(params: Any) = {
    //    val heartBeatActor = context.actorOf(Props(HeartBeatActor(self, SmsSystem.IS_LINUX)), name = SmsSystem.HEARTBEAT_ACTOR_NAME)
    //初始化调度器用于周期性任务调度，调度时间最小周期等于心跳时间
    val heartbeatScheduler = context.system.scheduler.schedule(heartbeatDelay, heartbeatInterval, self, HeartbeatMsg())(context.system.dispatcher)
    cancellableList += heartbeatScheduler
    1
  }

  /**
   * 收到stateMsg后的处理逻辑，Master收到的stateMsg主要来自各个slave节点
   * 收到消息后更新slave对应actor的状态
   * @param state 初始化状态
   */
  def stateChanged(state: Int, cause: Any) {
    val actorPath = sender.path.toString
    updateActorState(actorPath, state)
    log.info(s"actor at : $actorPath is update to state: $state due to $cause")
  }

  /**
   * 同步更新一个actor的状态到整个集群
   * @param actorPath
   * @param state
   */
  def updateActorState(actorPath: String, state: Int) {
    //更新本地状态
    routingService.updateState(actorPath, state) match {
      case Some(lst) => lst.foreach {
        //同步actor状态到所有slave
        updatedConf =>
          routingService.getRooting(SLAVE_BUSS_PATH).foreach {
            slave =>
              context.actorSelection(slave.actorPath) ! RoutingUpdateMsg(actorPath, updatedConf)
              log.info(s"send a  RootingUpdateMsg of : $actorPath for state: $state to ${slave.actorPath}")
          }
      }
      case _ => //ignore
    }
  }

  /**
   * master收到AddMsg后会在指定的部署逻辑上创建一个actor
   * 创建方式是给该路径上的slave节点发送一条包含配置参数ActorConfig的AddMsg
   * @param actorId 被创建的Actor的Id。默认也会作为其ActorName
   * @param slavePath 被创建的actor所属的slave路径
   * @param clazz 创建的actor的class类型， slavePath下的JVM必须能加载到这个类
   * @param parameters actor的初始化参数
   * @return
   */
  def add(actorId: String, slavePath: String, clazz: String, parameters: Any): Int = {
    if (slavePath.equalsIgnoreCase("localhost") || slavePath == self.path.toString) {
      //slavePath为null时添加到本地
      val clz = Class.forName(clazz)
      val act = if (parameters != null)
        context.actorOf(Props(clz, parameters), actorId)
      else context.actorOf(Props(clz), actorId)
      act ! InitMsg(parameters)
      Deployment.DEPLOYED_NORMAL
    } else {
      //添加到远程slave
      val conf = ActorConfig(id = actorId, name = actorId, clazzName = clazz)
        .withParams(parameters)
        .withDeploy(Deployment(parentPath = slavePath, path = slavePath + "/" + actorId, deployName = actorId))
      addActor(conf)
    }
  }

  /**
   * 一个slave加入master的处理逻辑
   * @param mPath master路径
   * @param actorPath 加入的Actor的路径
   */
  def join(mPath: String, actorPath: String) {

    def getConfigurations(actorPath: String) = {
      getChildrenConfig(actorPath) match {
        case lst if (lst != null && lst.nonEmpty) => lst.filter(conf => conf ne null) match {
          case fLst if (fLst.length > 0) => SlaveParams(fLst)
          case other => null
        }
        case other => null
      }

    }
    val slavePath = if (actorPath ne null) actorPath else sender.path.toString
    val params = getConfigurations(slavePath)
    val actor = context.actorSelection(slavePath)
    actor ! InitMsg(params) //初始化
    //slave 作为Actor一样加入到rooting中
    val slaveConfig = ActorConfig(id = SLAVE_NAME, name = "", clazzName = "").withDeploy(slavePath).withState(Deployment.DEPLOYED_NORMAL)
    routingService.removeRooting(slavePath) //去重
    routingService.addRooting(slaveConfig)
    actor ! RoutingSyncMsg(routingService.getAllRooting) //同步数据
    log.info("A node has joined the cluster:" + slavePath)
    log.info("current nodes:")
    routingService.getRooting(SLAVE_BUSS_PATH).foreach(conf => log.debug(conf.toString))
  }

  /**
   * 一个actor离开master处理逻辑
   * remove a slave from this master
   */
  def left(actorPath: String) {
    routingService.removeRooting(actorPath)
    routingService.find(conf => conf.deploy.parentPath == actorPath).foreach(actor => updateActorState(actor.actorPath, Deployment.UN_DEPLOYED))
    log.info("An Actor has left the cluster " + actorPath)
    log.info("current nodes:")
    routingService.getRooting(actorPath).foreach(conf => log.info(conf.toString))
  }

  /**
   * 心跳消息处理逻辑
   * @param actor 发送心跳的actor
   * @param msg 心跳消息
   */
  def heartBeat(actor: ActorRef, msg: HeartbeatMsg) {
    actor.path.toString match {
      case "akka://masterSys/deadLetters" => heartbeatTicking(msg) //收到调度器消息，进行心跳超时检查
      case ph if (ph != self.path) => {
        //处理slave心跳消息
        val slavePath = actor.path.parent.toString
        log.debug("receive heartbeat from " + slavePath)
        heartbeatReceived(slavePath, msg)
        if (persistenceActor ne null)
          persistenceActor ! msg
      }
      case _ => unhandled()
    }
  }

  /**
   * 更新消息处理逻辑，默认提供空实现，由子类实现
   * @param actorPath 配置对应的actor地址
   * @param param 更新后的参数
   * @param discardOld 是否废弃原参数
   * @param propagate 是否传播更新
   */
  def update(actorPath: String, confId: String, param: Any, discardOld: Boolean, propagate: Boolean) {
    val realPath = if (actorPath.toLowerCase().startsWith("localhost")) actorPath.toLowerCase().replaceFirst("localhost", self.path.toString) else actorPath
    configExtension.updateParam(realPath, param)
    if (propagate)
      context.actorSelection(realPath) ! UpdateMsg(realPath, confId, param, discardOld)
  }

  /**
   * 停止消息处理逻辑
   * @param parent 停止的actor的parent，如果为空就是停止自己
   * @param name 被停止的actor name
   */
  def stop(parent: String, name: String) {
    val actorPath = s"$parent/$name"
    parent match {
      case "localhost" | "LOCALHOST" => stopLocalActor(name)
      case masterPath:String if (self.path.toString == masterPath) => stopLocalActor(name)
      case slavePath: String if (!slavePath.isEmpty()) => try {
        routingService.removeRooting(actorPath) //删除本地rooting
        if (routingService.getRooting(SLAVE_BUSS_PATH).exists(_.actorPath == slavePath) && (name ne null)) {
          routingService.getRooting(SLAVE_BUSS_PATH).foreach {
            // 同步删除所有slave的rooting
            slave => context.actorSelection(slave.actorPath) ! RoutingRemove(actorPath)
          }
          context.actorSelection(parent) ! StopMsg(parent, name) //停止actor
        }
      }
      case x: Any => unhandled(x)
    }
  }

  /**
   * slave心跳超时的处理逻辑
   * @param watchee 被监控的slave路径
   */
  def handleHeartBeatTimeout(watchee: String) {
    log.error(s"Receive slave heartbeat time out during ${heartbeatInterval.toMillis * hbTimeoutFactor} salvePath: $watchee")
    watchList -= watchee
    left(watchee)
    log.warning(s"salve: $watchee has been removed from slave list")
  }

  // 下面都是内部使用的方法
  // private used method
  //

  /**
   * 添加一个actor，根据actor.deploy.parent配置到对应的slave下，如果该slave已启动则发送消息触发slave创建Actor
   * @param conf actor配置
   * @return
   */
  private[this] def addActor(conf: ActorConfig): Int = {
    configExtension.addConf(conf)
    routingService.addRooting(conf)
    val slaveSeq = routingService.getRooting(SLAVE_BUSS_PATH)
    slaveSeq.map {
      slaveConf => context.actorSelection(slaveConf.actorPath) ! RoutingAddMsg(slaveConf)
    }
    if (slaveSeq.exists(_.actorPath == conf.deploy.parentPath)) {
      //如果slave已经在当前列表中，则添加节点
      val future = context.actorSelection(conf.deploy.parentPath) ? AddMsg(name = conf.name, conf.deploy.parentPath, conf.clazzName, params = conf.params)
      Await.result(future, timeout.duration).asInstanceOf[Int]
    } else
      Deployment.UN_DEPLOYED
  }

  /**
   * add actor to a remote host directly
   */
  private[this] def addRemoteActor(name: String, clazzName: String, conf: ActorConfig) {
    import scala.language.existentials
    val address = AddressFromURIString(conf.deploy.path)
    val clazz = Class.forName(clazzName).getClass
    context.system.actorOf(Props(clazz).withDeploy(Deploy(scope = RemoteScope(address))), name)
    configExtension.addConf(conf)
    routingService.addRooting(conf)
  }

  private[this] def getChildrenConfig(slavePath: String) = {
    configExtension.getSlaveConfList(slavePath, routingService)
  }

  private[this] def stopLocalActor(name: String) = {
    context.child(name) match {
      //删除本地actor
      case Some(act) =>
        context.stop(act); true
      case _ => false
    }
  }

}

/**
 * master-slave协议中slave端消息处理实现
 */
trait SlaveExtensionImpl extends MasterSlaveActorExtension with RoutingService {

  this: Actor =>

  var masterPath: String

  val actorType = Slave

  val cancellableList: mutable.ListBuffer[Cancellable] = mutable.ListBuffer()
  val heartbeatDelay = Duration.Zero
  val heartbeatInterval = Duration(15000, TimeUnit.MILLISECONDS)

  def init(params: Any) = try {
    val heartBeatActor = context.actorOf(Props(new HeartBeatActor(masterPath, SmsSystem.IS_LINUX)), name = SmsSystem.HEARTBEAT_ACTOR_NAME)
    val heartbeatScheduler = context.system.scheduler.schedule(heartbeatDelay, heartbeatInterval, heartBeatActor, HeartbeatMsg())(context.system.dispatcher)
    cancellableList += heartbeatScheduler
    params match {
      case msg: SlaveParams =>
        initConfig(msg); 1
      case _ => log.warning("Init Actor :" + context.self + "with unhandled configuration:" + params); 0
    }
  } catch {
    case e: Throwable => log.error("Init Actor :" + context.self + "with exception" + e); 0
  }

  def initConfig(msg: SlaveParams) {
    msg.params match {
      case null => unhandled()
      case lst: List[ActorConfig] => {
        for (conf <- lst if conf ne null) {
          log.info("Init Actor:" + context.self + "with parameters:" + conf.params)
          add(conf.name, conf.deploy.parentPath, conf.clazzName, conf.params)
        }
      }
    }
  }

  def join(actPath: String, sPath: String) {
    masterPath = if (actPath != null && !actPath.isEmpty) actPath; else masterPath
    log.info("received a join msg to master:" + masterPath)
    if (masterPath ne null) context.actorSelection(masterPath) ! JoinMsg()

  }

  def left(actPath: String) {
    context.actorSelection(masterPath) ! LeftMsg(actPath)
  }

  def add(name: String, path: String, clazzName: String, params: Any) = {
    val clazz = Class.forName(clazzName)
    val actorRef = {
      if (params != null) context.actorOf(Props(clazz, params), name)
      else context.actorOf(Props(clazz), name)
    }
    //    actorRef tell(InitMsg(params), context.actorFor(masterPath))
    actorRef ! InitMsg(params)
    showChildren()
    1
  }

  def stop(parent: String, name: String) {
    parent match {
      case null => {
        for (child <- context.children)
          context.stop(child)
        for (cancellable <- cancellableList)
          cancellable.cancel()
        if (masterPath ne null) context.actorSelection(masterPath) ! LeftMsg()
        context.stop(self)
        log.info(s"Actor: ${self.toString()} has been stopped.")
      }
      case path if (name ne null) => context.child(name) match {
        case Some(actor) => actor ! StopMsg(null, name)
        case None => log.error(s"Actor: $name doesn't exist on parent ${self.path}.")
      }
    }

  }

  def stateChanged(state: Int, cause: Any) {
    val actorPath = sender.path.toString
    if (actorPath == masterPath) // 收到来自master 的通知进行节点状态修改
      routingService.updateState(actorPath, state)
    else // 其他节点的状态改变告知master
      context.actorSelection(masterPath).tell(StateMsg(state, cause), sender)
  }

  def update(actorPath: String, confId: String, param: Any, discardOld: Boolean, propagate: Boolean) {

  }

  def heartBeat(actor: ActorRef, msg: HeartbeatMsg) {

  }

  def showChildren() {
    log.info("current children:\n" + context.children.mkString("\n"))
  }

}

/**
 * 工作节点基类实现
 */
trait WorkActorExtension extends MasterSlaveActorExtension {
  this: Actor =>

  import context._

  val actorType = Worker

  /**
   * 子类需实现的业务消息处理逻辑
   * process business message
   */
  def process: PartialFunction[Any, Unit]

  /**
   * 初始话这个worker为正常工作状态
   * 初始化正确后work会将自己的状态告知源节点（一般为slaveActor）
   * @param params 初始化参数
   */
  final def init(params: Any) = {
    try {
      val state = initParams(params)
      if (state == 1) {
        become(activated, discardOld = false) // become activated after initiated
        //sender ! StateMsg(Deployment.DEPLOYED_NORMAL, "actor initiated.")
        stateChanged(Deployment.DEPLOYED_NORMAL, "actor initiated.")
        log.info("become activated..")
      } else log.info(s"initiation failed: $state")
      state
    } catch {
      case ex: Throwable =>
        stateChanged(Deployment.DEPLOYED_UN_INITIATED, ex)
        log.error(ex, "init encounter an exception")
        0
    }
  }

  def initParams(params: Any) = {
    if (params != null)
      log.info(s"Init Actor:${context.self} with parameters: ${params.getClass}")
    1
  }

  /**
   * 主消息处理逻辑，适用于actor初始状态
   * main message handling hook, the initial state of a actor
   */
  override def handleMasterSlaveMsg(msg: MasterSlaveProtocol) {
    msg match {
      case InitMsg(params) => init(params)
      case UpdateMsg(path, configId, config, discardOld, propagate) =>
        update(path, configId, config, discardOld, propagate)
        log.info(s"Actor: ${self.toString()} received a update msg with param: $config.")
      case SuspendMsg(_) => become(inActive) // become inActivated after suspended
      case JoinMsg(slavePath, masterPath) => log.info("received a join msg to master:" + sender)
      case StopMsg(parent, name) => stop(parent, name)
      case _ => log.warning("received a unhandled message.")
    }
  }

  /**
   * inActive状态下的消息处理逻辑
   * only handle master message on inActive state
   */
  def inActive: Receive = {
    case msg: MasterSlaveProtocol => msg match {
      case _: InitMsg => become(activated, discardOld = true) // become activated after initiated
      case StopMsg(parent, name) => stop(parent, name)
    }
    case _ => log.warning("uninitiated Actor.")
  }

  /**
   * active状态下增加业务处理逻辑，由子类自己实现
   * when be activated this method will be push to  the top of process stack
   */
  def activated: Receive = {
    case msg: MasterSlaveProtocol => handleMasterSlaveMsg(msg)
    case msg: Any => try process(msg) catch logEx(log, (self.path.toString,msg) )

  }

  /**
   * work 调用这个方法时会向parent转发状态改变的消息，让parent处理
   * @param state 状态
   * @param cause 原因
   */
  def stateChanged(state: Int, cause: Any) {
    context.parent ! StateMsg(state, cause)
  }

  /**
   * 停止这个actor及其所有子actor
   */
  def stop(parent: String, name: String) {
    for (child <- context.children) {
      context.stop(child)
    }
    context.stop(self)
    log.info(s"Actor: ${self.toString()} has been stopped.")
  }

  def update(path: String, configId: String, param: Any, discardOld: Boolean, propagate: Boolean) {

  }

  //
  // worker 暂时不处理下面几种消息，调用时会报错
  // 子类需要扩展处理时可以重写这几个方法
  //

  def add(actorId: String, slavePath: String, clazz: String, parameters: Any): Int = ???

  def left(actorPath: String) = ???

  def join(masterPath: String, actorPath: String) = ???

  def heartBeat(actor: ActorRef, msg: HeartbeatMsg) = ???
}

trait MasterSlaveActorType

case object Master extends MasterSlaveActorType

case object Slave extends MasterSlaveActorType

case object Worker extends MasterSlaveActorType
