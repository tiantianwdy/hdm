package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext.hdmToKVHDM
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * Created by tiantian on 7/01/15.
  */
class HDMPrimitiveBenchmark(val context:String) {

   def init(context:String, localCores:Int = 0): Unit ={
     HDMContext.init(leader = context, cores = localCores)
     Thread.sleep(100)
   }


   def testCount(dataPath:String,  parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.count()

     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testTop(dataPath:String,  k:Int,  parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       as(1).toInt
     }.top(k)

     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testMap(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       if(keyLen > 0) (as(0).substring(0,keyLen), as(1).toInt)
       else (as(0), as(1).toInt)
     }
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testMapCount(dataPath:String,  parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       (as(0), as(1).toInt)
     }.count()
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testMapAll(): Unit ={


   }

   def testMapSelect(): Unit ={

   }

   def testGroupBy(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={

     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",");
       if(keyLen > 0) (as(0).substring(0,keyLen), as(1).toInt)
       else (as(0), as(1).toInt)
     }.groupBy(_._1)
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testReduceByKey(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",");
       if(keyLen > 0) (as(0).substring(0,keyLen), as(1).toInt)
       else (as(0), as(1).toInt)
     }.reduceByKey((t1,t2) => t1 + t2)
       //.groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))


     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }

   }

  def testGroupByReduce(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
    val path = Path(dataPath)
    val hdm = HDM(path)

    val start = System.currentTimeMillis()
    val wordCount = hdm.map{ w =>
      val as = w.split(",");
      if(keyLen > 0) (as(0).substring(0,keyLen), as(1).toInt)
      else (as(0), as(1).toInt)
    }.groupBy(_._1).map(t => (t._1, t._2.map(_._2).reduce(_+_)))

    wordCount.compute(parallelism) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
        hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

  }

 }


