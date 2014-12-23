package org.nicta.hdm.io

/**
 * Created by Tiantian on 2014/12/22.
 */
import org.apache.hadoop.conf._;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.{FSDataInputStream, Path, FileSystem, FileStatus}
;

import java.io.{BufferedReader, DataInput}
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.mapred.LineRecordReader
import org.junit.Test
;

class HadoopIOTest {

  @Test
  def dataReaderTest(){
    val dataInput = new DataInputBuffer()
    val reader = new LineRecordReader(dataInput)
    val bufferReader = new BufferedReader()
    val conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020/user/hbase");
    conf.set("hadoop.job.ugi", "hbase");
    val fs = FileSystem.get(conf);

    //todo check whether this path can involve the block index
    val fileStatus = fs.getFileStatus(new Path(""))
    val blockLocations = fs.getFileBlockLocations(fileStatus, 0 , fileStatus.getLen)
    val loc = blockLocations.head
    val hosts = loc.getHosts
    // ask remote worker on hosts to load the data
    loc.readFields(dataInput)

    val buffer = new Array[Byte](fileStatus.getLen)
    dataInput.readFully(buffer)
    buffer
  }


}
