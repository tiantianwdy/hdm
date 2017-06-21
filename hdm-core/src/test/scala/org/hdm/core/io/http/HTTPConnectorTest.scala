package org.hdm.core.io.http

import java.io.StringWriter

import org.apache.commons.io.IOUtils
import org.junit.{Ignore, Test}

/**
  * Created by wu056 on 13/06/17.
  */
class HTTPConnectorTest {

  val connector = new HTTPConnector


  @Test
  def testSendGet(): Unit ={
    connector.sendGet("http://google.com.au", (entity) => {
      val is = entity.getContent
      val writer = new StringWriter()
      IOUtils.copy(is, writer)
      writer.toString.split("\n").foreach(println(_))
    })
  }


  @Ignore("need a testing http server which support http post.")
  @Test
  def testSendPost(): Unit ={
    connector.sendPost("http://google.com.au/search", Seq(("q", "data61"), ("op", "data61") ), (entity) => {
      val is = entity.getContent
      val writer = new StringWriter()
      IOUtils.copy(is, writer)
      writer.toString.split("\n").foreach(println(_))
    })
  }

}
