package org.hdm.core.server

import org.hdm.core.executor.{AppContext, HDMContext}
import org.hdm.core.io.Path
import org.hdm.core.model.HDM

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Created by Tiantian on 2014/12/19.
 */
class ServerStartTest {



}

object ServerStartMaster{

  def main(args:Array[String]): Unit ={
    HDMServer.main(Array("-m", "true", "-p", "8999"))
  }
}


object ServerStartSlave1{

  def main(args:Array[String]): Unit ={
    HDMServer.main(Array("-m", "false", "-p", "10001", "-P", "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"))
  }
}

object ServerStartSlave2{

  def main(args:Array[String]): Unit ={
    HDMServer.main(Array("-m", "false", "-p", "10002", "-P", "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"))
  }
}

object JobRunningTest{

  import ExecutionContext.Implicits.global

  val hDMContext = HDMContext.defaultHDMContext

  val appContext = new AppContext()

  def main(args:Array[String]): Unit ={
    hDMContext.init(leader ="akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster", slots =0)
    Thread.sleep(1000)
    //        val hdm = HDM.horizontal(text, text2)
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
    val hdm = HDM(path)

    val wordCount = hdm.map{ w =>
      val as = w.split(",");
      (as(0).substring(0,3), as(1).toInt)
    } //.groupBy(_._1)
    .groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))
      //.map(w => (w.split(","))).map(as => (as(0).substring(0,3),as(1).toInt)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

    val topK = hdm.map{ w =>
      val as = w.split(",");
      as(1).toInt
    }.top(10)

    val count = hdm.count()

    val mapCount = hdm.map{ w =>
      val as = w.split(",");
      (as(0).substring(0,3), as(1).toInt)
    }.count()

    val start = System.currentTimeMillis()
    topK.compute(4) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
//        hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

  }

}