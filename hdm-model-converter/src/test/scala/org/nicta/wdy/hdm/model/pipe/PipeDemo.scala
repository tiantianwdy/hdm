package org.nicta.wdy.hdm.model.pipe


import com.sun.org.apache.bcel.internal.classfile.Collector
import org.apache.spark.rdd.RDD
import org.junit.Test
import org.nicta.wdy.hdm.model.converter.{SparkPipeEntry, SparkPipe}

/**
 * Created by tiantian on 14/07/15.
 */
class PipeDemo {
  import scala.collection.JavaConversions._

  @Test
  def testPipe(): Unit ={
    val pipe1 = new SparkPipe("spark-pipe-1.0.0",
      "127.0.0.1:8999",
      "spark://127.0.0.1:7077",
      "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings",
      "text",
      "hdfs://127.0.0.1:9001/app/spark-pipe/1.0.0/",
      "text",
      (in:RDD[String]) => in.map(_.split(",")).map(str => str(0)-> str(1))
       )

    val classes = Collector.getClassesUsedBy(pipe1.getClass.getName, "")
    for (cl <- classes) {
      println(cl.toString)
    }
//    SparkPipeEntry.pipe = pipe1
//    SparkPipeEntry.main(null)
  }

}
