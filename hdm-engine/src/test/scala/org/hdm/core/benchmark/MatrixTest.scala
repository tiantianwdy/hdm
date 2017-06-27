package org.hdm.core.benchmark

import org.hdm.core.context.{HDMServerContext, AppContext}
import org.junit.Ignore

/**
 * Created by tiantian on 6/05/16.
 */
@Ignore("Not ready yet.")
class MatrixTest {

  val hDMContext = HDMServerContext.defaultContext

  val appContext = new AppContext(masterPath = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")





}
