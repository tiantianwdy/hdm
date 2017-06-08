package org.hdm.core.server

import org.junit.Test
/**
  * Created by tiantian on 22/05/17.
  */
class ClusterServerTest {

  @Test
  def testStartClusterMaster(): Unit ={
    HDMServer.main(Array("-m", "true", "-p", "8999", "-n", "cluster"))
    Thread.sleep(1000000)
  }

  @Test
  def testStartClusterSlave(): Unit ={
    HDMServer.main(Array("-m", "false", "-p", "10010", "-n", "cluster", "-P", "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster", "-s", "4", "-mem", "2G"))
    Thread.sleep(1000000)
  }

}
