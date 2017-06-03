package org.hdm.akka.configuration

/**
 * @author wudongyao
 * @date 2013-7-9
 * @version 0.0.1
 *
 */
trait Parameters extends Serializable {

}

/**
 * Actor 配置参数
 *
 * @param id   actor的标识，无实际业务功能
 * @param name  actor名字，用于生成路径
 * @param clazzName  actor的class，用于创建Actor
 * @param topic   actor订阅的topic
 * @param params  actor的初始化参数
 * @param deploy  actor的部署参数
 */
case class ActorConfig(
  id: String,
  name: String,
  clazzName: String,
  topic: String = "",
  params: Any = "",
  deploy: Deployment = Deployment(path = "")) extends Serializable {

  def withDeploy(deploy: Deployment) = this.copy(deploy = deploy)

  /**
   * 更新actorConfig的部署路径parentPath=[newPath], path=[newPath]/[ActorConfig.name]
   * @param parentPath: String 新路径的parentPath
   */
  def withDeploy(parentPath: String): ActorConfig = withDeploy(deploy.copy(parentPath = parentPath,
    path = {
      if (!name.isEmpty()) s"$parentPath/$name"
      else parentPath
    }))

  def withParams(params: Any) = this.copy(params = params)

  def withState(state: Int) = this.copy(deploy = deploy.copy(state = state))

  def actorPath = {
    deploy match {
      case depy if (depy ne null) => deploy.path match {
        case path if (path ne null) => deploy.path
        case null if (deploy.parentPath ne null) => s"${deploy.parentPath}/${name}"
        case _ => ""
      }
      case _ => ""
    }
  }
}

/**
 * actor部署配置
 *
 * @param path  actor的绝对路径
 * @param parentPath  actor 的父路径
 * @param deployName actor的部署名，一般情况下等于ActorName
 * @param state   actor部署状态
 */
case class Deployment(
  path: String,
  parentPath: String = "",
  deployName: String = "",
  state: Int = Deployment.UN_DEPLOYED) extends Serializable

/**
 * Actor部署状态列表
 * Router选择分发节点时会根据状态优先级从高至低选择
 */
object Deployment {

  val DEPLOYED_NORMAL = 1 //已部署,正常状态
  val DEPLOYED_BUSY = 0 //已部署,繁忙状态
  val DEPLOYED_SUSPEND = -1 //已部署,挂起状态
  val DEPLOYED_ERROR = -2 //已部署,错误状态
  val DEPLOYED_UN_INITIATED = -3 // 已部署但未初始化
  val UN_INITIATED = -4 //未初始化
  val UN_DEPLOYED = -5 //未部署

  val nameMap = Map(
    UN_INITIATED -> "UN_INITIATED",
    UN_DEPLOYED -> "UN_DEPLOYED",
    DEPLOYED_UN_INITIATED -> "DEPLOYED_UN_INITIATED",
    DEPLOYED_NORMAL -> "DEPLOYED_NORMAL",
    DEPLOYED_BUSY -> "DEPLOYED_BUSY",
    DEPLOYED_SUSPEND -> "DEPLOYED_SUSPEND",
    DEPLOYED_ERROR -> "DEPLOYED_ERROR"
  )

  def name(state:Int) = {
    nameMap(state)
  }
}