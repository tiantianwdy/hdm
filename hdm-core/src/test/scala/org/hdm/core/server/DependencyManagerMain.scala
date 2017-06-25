package org.hdm.core.server

import java.io.File
import org.hdm.core.executor.DynamicDependencyThreadFactory
import org.hdm.core.server.HDMServerContext

/**
 * Created by tiantian on 8/03/16.
 */
object DependencyManagerMain {

  def main(args: Array[String]): Unit ={

    HDMServerContext.defaultContext.init(slots = 0) // start master
    val start = System.currentTimeMillis()
    val file = "/home/tiantian/Dev/lib/hdm/HDM-benchmark-0.0.1.jar"
    val url = new File(file).toURI.toURL
    DynamicDependencyThreadFactory.addGlobalDependency(Array(url))
//    DependencyManager.loadGlobalDependency(Array(url))
    val end = System.currentTimeMillis() - start
    println(s"loading completed in $end ms.")
  }
}
