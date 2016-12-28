package org.hdm.core.io

import java.nio.file.{StandardOpenOption, OpenOption, Paths, Files}

import org.apache.hadoop.hdfs.web.JsonUtil
import org.junit.Test
import org.hdm.core.io.{JacksonUtils, JsonObjectSerializer}

import scala.beans.BeanProperty

/**
 * Created by tiantian on 22/11/16.
 */
class JsonParserTest {

  val parser = new JsonObjectSerializer[Ranking]()


  @Test
  def testJSONReader(): Unit ={
    val location = "/home/tiantian/Dev/test/data/json/ranking-part-00001.json"
    val path = Paths.get(location)
    val fileInputStream = Files.newInputStream(path)
    val results = parser.fromInputStream(fileInputStream)
    results foreach (println(_))
    fileInputStream.close()

  }

  @Test
  def testJSONWriter(): Unit = {
    val data = Seq.fill(10){
      new Ranking("hdfs://test/data", 1.1F)
    }
    val location = "/home/tiantian/Dev/test/data/json/ranking-part-00003.json"
    val path = Paths.get(location)
    val fileOutputStream = Files.newOutputStream(path, StandardOpenOption.TRUNCATE_EXISTING)
//    val jsonWriter = JacksonUtils.getJSONWriter(fileOutputStream)
//    data foreach {jsonWriter.writeObject(_)}
    parser.toOutputStream(data, fileOutputStream)
  }


}


class Ranking(@BeanProperty var url:String ,
              @BeanProperty var ranking:Float) extends Serializable {

  def this(){
    this("", 0F)
  }

}