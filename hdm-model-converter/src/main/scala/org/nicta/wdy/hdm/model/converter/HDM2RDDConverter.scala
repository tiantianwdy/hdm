package org.nicta.wdy.hdm.model.converter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Dependency, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.nicta.wdy.hdm.functions.{ParGroupByFunc, ParReduceFunc, ParMapFunc}
import org.nicta.wdy.hdm.model.HDM

import scala.reflect.ClassTag

/**
 * Created by tiantian on 6/07/15.
 */
object HDM2RDDConverter extends ModelConverter[HDM[_, _], RDD[_]]{

  implicit def hdm2Rdd[T:ClassTag, R:ClassTag](hdm:HDM[T,R]):RDD[R] = {
    val source = "hdm.root.input"
    val sparkConf = new SparkConf().setAppName("SparkHDFSBenchmark")
    val hadoopConf = new Configuration()
    val sc = new SparkContext(sparkConf)
    val rdd:RDD[_] = hdm.func match {
      case mf:ParMapFunc[T,R] => sc.objectFile[T](source).map(mf.f(_))
      case gb:ParGroupByFunc[T,R] => sc.objectFile[T](source).groupBy(gb.f(_))
//      case rf:ParReduceFunc[R, R] => sc.objectFile[R](source).reduce(rf.f)
    }
    rdd.asInstanceOf[RDD[R]]
  }

}


abstract class TypedRDD[T:ClassTag]( @transient private var sc: SparkContext,
                            @transient private var deps: Seq[Dependency[_]]) extends RDD[T](sc,deps) {



}