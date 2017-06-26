package org.hdm.core.executor

import java.util
import java.util.concurrent.{Executors, TimeUnit, ExecutorService, Callable, Future}

import org.hdm.core.utils.DynamicDependencyThreadFactory

import scala.concurrent.{ExecutionContext}
import akka.actor.{ActorSystem, Actor}

import org.hdm.core.coordinator.Coordinator
import akka.util.Timeout
import scala.concurrent.duration.{Duration, DurationInt}
import com.typesafe.config.ConfigFactory

/**
 * Created by Tiantian on 2014/12/2.
 */
abstract class ClusterExecutorService(val localExecutor: ExecutorService) extends ExecutorService with Coordinator {

  override def execute(command: Runnable): Unit = ???

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T = ???

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]]): T = ???

  override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): util.List[Future[T]] = ???

  override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]]): util.List[Future[T]] = ???

  override def submit(task: Runnable): Future[_] = ???

  override def submit[T](task: Runnable, result: T): Future[T] = ???

  override def submit[T](task: Callable[T]): Future[T] = ???

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = ???

  override def isTerminated: Boolean = ???

  override def isShutdown: Boolean = ???

  override def shutdownNow(): util.List[Runnable] = ???

  override def shutdown(): Unit = ???
}


class AkkaClusterExecutor (val executorSer: ExecutorService,
                           val actorSys:ActorSystem ) extends ClusterExecutorService(localExecutor = executorSer) {



  import akka.pattern._

  implicit val executor = ExecutionContext.fromExecutorService(localExecutor)

  implicit val timeout = Timeout(10 seconds)

  implicit val maxWaitResponseTime = Duration(10, TimeUnit.SECONDS)

  override def send(path:String, msg:Serializable): Unit = {
    actorSys.actorSelection(path) ! msg
  }

  override def request(path:String, msg: Serializable): concurrent.Future[Serializable] = {
    val res = actorSys.actorSelection(path) ? msg
    res.map(_.asInstanceOf[Serializable])
  }

  override def broadcast(msg: Serializable): Unit = ???

  override def declare(msg: Serializable): Unit = ???

  override def vote(msg: Serializable): Unit = ???

  override def leave(): Unit = ???

  override def join(entryPath: String): Unit = ???
}

object ClusterExecutorContext {

  def apply():ExecutionContext = {
    val cores = Runtime.getRuntime.availableProcessors
    apply(cores)
  }

  def apply(numCores: Int):ExecutionContext = {
//    apply(Executors.newFixedThreadPool(numCores + 1))
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numCores + 1, DynamicDependencyThreadFactory()))
  }

  def apply(localExecutor: ExecutorService):ExecutionContext = {
    ExecutionContext.fromExecutorService(new AkkaClusterExecutor(localExecutor, ActorSystem("hdm", ConfigFactory.load())))
  }

}
