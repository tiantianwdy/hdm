package org.nicta.wdy.hdm.server

import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.message.AskCollaborateMsg

/**
 * Created by tiantian on 22/05/16.
 */
object HDMClient {

  /**
   * Example:
   *     val master = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
   *     val file = "/home/tiantian/Dev/workspace/hdm/hdm-benchmark/target/HDM-benchmark-0.0.1.jar"
   * @param master
   * @param app
   * @param version
   * @param file
   * @param user
   */
  def submitApplication(master:String, app:String, version:String, file:String, user:String): Unit ={
    val start = System.currentTimeMillis()

    SmsSystem.startAsClient(master, 20001)
    Thread.sleep(100)
    DependencyManager.submitAppByPath(master, app, version, file, user)
    Thread.sleep(3000)
  }


  /**
   * Example:
   * val master1 = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/ClusterExecutor"
   * val master2 = "akka.tcp://masterSys@127.0.1.1:8998/user/smsMaster/ClusterExecutor"
   *
   * @param master1
   * @param master2
   */
  def coordinate(master1: String, master2: String): Unit = {
    SmsSystem.startAsClient(master1, 20001)
    val msg = AskCollaborateMsg(master1)
    SmsSystem.forwardMsg(master2, msg) // ask master 2 to join master 1
    Thread.sleep(2000)
  }

  def main(args: Array[String]) {
    try {
      assert(args.length > 0)

      val argSeq = args.toSeq
      val cmd = argSeq.head
      val params = argSeq.tail
      cmd match {
        case "submit" =>
          assert(params.length >= 5, "Submit Usage: submit [master] [appName] [version] [filePath] [userName]")
          submitApplication(params(0), params(1), params(2), params(3), params(4))
        case "coordinate" =>
          assert(params.length >= 2, "Coordinate Usage: coordinate [master1] [master2]")
          coordinate(params(0), params(1))
      }
      System.exit(0)
    } catch {
      case x: Throwable =>
        System.err.println(x)
        System.exit(1)
    }

  }

}
