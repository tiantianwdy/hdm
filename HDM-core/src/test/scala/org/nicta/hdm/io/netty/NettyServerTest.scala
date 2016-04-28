package org.nicta.hdm.io.netty

import org.junit.{Before, Test}

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.netty.NettyBlockServer
import org.nicta.wdy.hdm.serializer.JavaSerializer
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}

/**
 * Created by tiantian on 28/05/15.
 */
class NettyServerTest {

  val serializer = new JavaSerializer(HDMContext.defaultConf).newInstance()
  val compressor = HDMContext.defaultHDMContext.getCompressor()
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

  @Test
  def testShutDown(): Unit ={
    blockServer.shutdown()
  }

}
