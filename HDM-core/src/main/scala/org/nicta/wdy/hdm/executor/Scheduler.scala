package org.nicta.wdy.hdm.executor

import scala.concurrent._
import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean
import org.jboss.netty.util.internal.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.nicta.wdy.hdm.storage.{HDMBlockManager, Computed, BlockManager}

/**
  * Created by Tiantian on 2014/12/1.
 */
trait Scheduler {

  implicit val executorService:ExecutionContext

  def submitJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]]

  def addTask[I,R](task:Task[I,R]):Promise[HDM[I,R]]

  def init()

  def start()

  def stop()

  protected def scheduleTask [I:ClassTag, R:ClassTag](task:Task[I,R]):Promise[HDM[I,R]]

}

class SimpleFIFOScheduler(implicit val executorService:ExecutionContext) extends Scheduler{

  import scala.concurrent.JavaConversions._

  val appBuffer:java.util.Map[String, ListBuffer[Task[_,_]]] = new ConcurrentHashMap[String, ListBuffer[Task[_,_]]]()

  val taskQueue = new LinkedBlockingQueue[Task[_,_]]()

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  val isRunning = new AtomicBoolean(false)

  override protected def scheduleTask[I:ClassTag, R:ClassTag](task: Task[I, R]): Promise[HDM[I, R]] = {
    val promise = promiseMap.get(task.taskId).asInstanceOf[Promise[HDM[I, R]]]
    // run job todo assign to remote node to execute this
    Future {
      //todo load remote input data in parallel threads rather than sequential
      val blks = task.call().map(bl => bl.id)
      val ref = HDMBlockManager().getRef(task.taskId) match {
        case dfm:DFM[I,R] => dfm.copy(blocks = blks, state = Computed)
        case ddm:DDM[R] => ddm.copy(state = Computed)
      }
      HDMBlockManager().addRef(ref)
      promise.success(ref.asInstanceOf[HDM[I, R]])
      triggerApp(task.appId)
    }
    promise
  }

  override def stop(): Unit = {
    isRunning.set(false)
  }

  override def start(): Unit = {
    isRunning.set(true)
    while(isRunning.get){
      val task = taskQueue.take()
      scheduleTask(task)
    }
  }

  override def init(): Unit = {
    isRunning.set(false)
    taskQueue.clear()
  }

  override def addTask[I, R](task: Task[I, R]): Promise[HDM[I,R]] = {
    val promise = Promise[HDM[I,R]]()
    promiseMap.put(task.taskId, promise)
    if(!appBuffer.containsKey(task.appId))
      appBuffer.put(task.appId, new ListBuffer[Task[_,_]])
    val lst = appBuffer.get(task.appId)
    lst += task
    triggerApp(task.appId)
    promise
  }

  //todo move and implement at job compiler
  override def submitJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]] = ???



  private def triggerApp(appId:String) = {
    if(appBuffer.containsKey(appId)){
      val seq = appBuffer.get(appId)
      synchronized{
        if(!seq.isEmpty) {
          //find tasks that all inputs have been computed
          val tasks = seq.filter(t =>
            if(t.input eq null) false
            else {
              t.input.forall(in =>
                HDMBlockManager().getRef(in.id).state.eq(Computed))
            }
          )
          if((tasks ne null) && !tasks.isEmpty){
            seq --= tasks
            tasks.foreach(taskQueue.put(_))
          }
        }
      }
    }

  }
}
