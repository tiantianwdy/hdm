package org.nicta.wdy.hdm.model.converter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}

/**
 * Created by tiantian on 13/07/15.
 */
object MRPipeEntry {

  var pipe : MRPipe[_, _, _, _] = _

  def main(args:Array[String]): Unit ={
    val conf = new Configuration()
    conf.set("job.end.notification.url", pipe.pipelineServer)
    conf.setInt("job.end.retry.attempts", 3)
    conf.setInt("job.end.retry.interval", 1000)

    val job = Job.getInstance(conf, pipe.name)
    job.setMapperClass(pipe.mapper.getClass)
    job.setCombinerClass(pipe.combiner.getClass)
    job.setReducerClass(pipe.reducer.getClass)
    FileInputFormat.addInputPath(job, new Path(pipe.inputPath))
    FileOutputFormat.setOutputPath(job, new Path(pipe.outputPath))

    job.submit()
    val completed = job.waitForCompletion(true)
    System.exit(if (completed) 0 else 1)
  }

}


