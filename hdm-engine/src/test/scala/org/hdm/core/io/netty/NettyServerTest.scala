package org.hdm.core.io.netty

import org.hdm.core.context.{HDMServerContext, HDMContext}
import org.hdm.core.serializer.JavaSerializer
import org.hdm.core.storage.HDMBlockManager
import org.junit.{After, Before, Test}

/**
 * Created by tiantian on 28/05/15.
 */
class NettyServerTest {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()
  val compressor = HDMServerContext.defaultContext.getCompressor()
  val blockServer = new NettyBlockServer(9091,
    4,
    HDMBlockManager(),
    serializer,
    compressor)

  @Before
  def beforeTest: Unit ={
    val thread = new Thread() {
      override def run(): Unit = {
        blockServer.init()
        blockServer.start()
      }
    }
    thread.setDaemon(true)
    thread.run()

  }

  @After
  def testShutDown(): Unit ={
    blockServer.shutdown()
  }

}
