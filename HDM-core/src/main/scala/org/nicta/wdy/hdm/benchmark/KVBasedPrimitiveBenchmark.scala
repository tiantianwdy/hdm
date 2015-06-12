package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * consider the input is text files with columns separated by ","
 * @param context context for running this benchmark
 * @param kIndex
 * @param vIndex
 */
class KVBasedPrimitiveBenchmark(val context:String, val kIndex:Int = 0, val vIndex:Int = 1) extends  Serializable{

   def init(context:String, localCores:Int = 0): Unit ={
     HDMContext.init(leader = context, slots = localCores)
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
     val kOffset = kIndex
     val vOffset = vIndex

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       as(vOffset).toFloat
     }.top(k)

     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
         System.exit(1)
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testMap(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)
     val kOffset = kIndex
     val vOffset = vIndex

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
       else (as(kOffset), as(vOffset).toFloat)
     }
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
         System.exit(1)
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

  def testMultipleMap(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit = {
    val path = Path(dataPath)
    val hdm = HDM(path)
    val kOffset = kIndex
    val vOffset = vIndex

    val start = System.currentTimeMillis()
    val wordCount = hdm.map{ w =>
      val as = w.split(",")
      (as(kOffset), as(vOffset).toFloat)
    }.map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(0,keyLen), d._2)}

    wordCount.compute(parallelism) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
        hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
        System.exit(1)
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }
  }

   def testMapCount(dataPath:String,  parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)
     val kOffset = kIndex
     val vOffset = vIndex

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       (as(kOffset), as(vOffset).toFloat)
     }.count()
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
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
     val kOffset = kIndex
     val vOffset = vIndex

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
       else (as(kOffset), as(vOffset).toFloat)
     }.groupBy(_._1)
     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
         System.exit(1)
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }
   }

   def testReduceByKey(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
     val path = Path(dataPath)
     val hdm = HDM(path)
     val kOffset = kIndex
     val vOffset = vIndex

     val start = System.currentTimeMillis()
     val wordCount = hdm.map{ w =>
       val as = w.split(",")
       if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
       else (as(kOffset), as(vOffset).toFloat)
     }
       .reduceByKey(_ + _)
//       .groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))


     wordCount.compute(parallelism) onComplete  {
       case Success(hdm) =>
         println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
         hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
         System.exit(1)
       case Failure(t) =>
         println("Job failed because of: " + t)
         t.printStackTrace()
     }

   }

  def testGroupMapValues(dataPath:String, keyLen:Int = 3, parallelism:Int = 4): Unit ={
    val path = Path(dataPath)
    val hdm = HDM(path)
    val kOffset = kIndex
    val vOffset = vIndex

    val start = System.currentTimeMillis()
    val wordCount = hdm.map{ w =>
      val as = w.split(",")
      if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
      else (as(kOffset), 1F)
    }.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _))
      //.map(t => (t._1, t._2.map(_._2).reduce(_+_)))

    wordCount.compute(parallelism) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
        hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
        System.exit(1)
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

  }

  def testFindByKey(dataPath:String, keyLen:Int = 3, parallelism:Int = 4, key:String): Unit ={
    val path = Path(dataPath)
    val hdm = HDM(path)
    val kOffset = kIndex
    val vOffset = vIndex

    val start = System.currentTimeMillis()
    val wordCount = hdm.map{ w =>
      val as = w.split(",")
      if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
      else (as(kOffset), as(vOffset).toFloat)
    }
    .groupBy(_._1).findByKey(_.startsWith(key))
      //.filter(t => t._1.startsWith("a")).groupBy(_._1)



    wordCount.compute(parallelism) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
        hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
        System.exit(1)
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

  }

  def testFindByValue(dataPath:String, keyLen:Int = 3, parallelism:Int = 4, value:Int): Unit ={
    val path = Path(dataPath)
    val hdm = HDM(path)
    val kOffset = kIndex
    val vOffset = vIndex

    val start = System.currentTimeMillis()
    val wordCount = hdm.map{ w =>
      val as = w.split(",")
      if(keyLen > 0) (as(kOffset).substring(0,keyLen), as(vOffset).toFloat)
      else (as(kOffset), as(vOffset).toFloat)
    }.groupBy(_._1).findValuesByKey(_._2 > value)


    wordCount.compute(parallelism) onComplete  {
      case Success(hdm) =>
        println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
        hdm.asInstanceOf[HDM[_,_]].blocks.foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

  }

 }


