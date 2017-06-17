package org.hdm.core.io.http

import org.hdm.core.io.Path
import org.hdm.core.io.reader.StringReader
import org.junit.Test

/**
  * Created by wu056 on 13/06/17.
  */
class HttpDataParserTest {

  implicit val serializer = new StringReader()
  val parser = new HTTPDataParser


  @Test
  def testReadStringBlock(): Unit ={
    val blk = parser.readBlock(Path("http://google.com.au"), null)
    blk.data.foreach(println(_))
  }

}
