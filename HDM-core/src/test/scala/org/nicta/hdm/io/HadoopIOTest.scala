package org.nicta.hdm.io

/**
 * Created by Tiantian on 2014/12/22.
 */

import java.io.{InputStreamReader, BufferedReader}
import java.net.URI
import java.nio.{ByteBuffer,DirectByteBuffer}

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem}
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.mapred.LineRecordReader
import org.junit.Test
import org.nicta.wdy.hdm.executor.{HDMContext, ClusterPlaner}

import org.nicta.wdy.hdm.io.{DataParser, HDFSUtils, Path}
import org.nicta.wdy.hdm.model.HDM

class HadoopIOTest {
  type HPath = org.apache.hadoop.fs.Path



  @Test
  def dataReaderTest(){
//    val dataInput = new DataInputBuffer()
//    val reader = new LineRecordReader(dataInput)

    val conf = new Configuration()
    val uri = URI.create("hdfs://127.0.0.1:9001/")
    conf.set("fs.default.name", "hdfs://127.0.0.1:9001")
//    conf.set("hadoop.job.ugi", "tiantian")
    val fs = FileSystem.get(conf)

    //todo check whether this path can involve the block index
    val status = fs.listStatus(new HPath("/user/spark/benchmark/1node/rankings"))
    var num = 0
    var count = 0
    val start = System.currentTimeMillis()
    status.foreach{s =>

      println(s.getPath.getName)
      if(s.isFile){
        val fileStatus = fs.getFileStatus(s.getPath)
        val blockLocations = fs.getFileBlockLocations(fileStatus, 0 , fileStatus.getLen)
        println("block size: " + fileStatus.getBlockSize)
        val loc = blockLocations.head
        val names = loc.getNames
        println("location:" + names(0))
        if(num < 10){
          num +=1
          val buffer = ByteBuffer.allocate(s.getBlockSize.toInt)
//          val buffer = new Array[Byte](s.getBlockSize.toInt)
          val inputStream = fs.open(s.getPath)
//          inputStream.read(buffer)
          val reader = new BufferedReader(new InputStreamReader(inputStream))
          var data = reader.readLine()
          while (data != null){
            val seq = data.split(",")
            count += 1
            data = reader.readLine()
          }
          //          println(new String(buffer.array().take(100)))
        }
      }
    }
    val end = System.currentTimeMillis() -start
    println(s"Read $num file totally $count lines within $end ms.")


  }


}
