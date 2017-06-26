package org.hdm.core.utils

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ThreadFactory}

/**
 * Created by tiantian on 7/03/16.
 */
class DynamicDependencyThreadFactory(private val under:ThreadFactory = Executors.defaultThreadFactory(),
                                     private val classLoader:DynamicURLClassLoader) extends ThreadFactory {

  val baseLoader = new AtomicReference[ClassLoader](classLoader)

  def addURLs(urls:Array[java.net.URL]): Unit = {
    urls.foreach(url => classLoader.addURL(url))
  }

  override def newThread(runnable: Runnable): Thread = {
    val thread = under.newThread(runnable)
    thread.setContextClassLoader(classLoader)
    thread
  }

}

object DynamicDependencyThreadFactory {

  import java.net.URL

  private val defaultLoader:DynamicURLClassLoader = {
//    val baseClassLoader = this.getClass.getClassLoader
    val baseClassLoader = Thread.currentThread().getContextClassLoader
    new DynamicURLClassLoader(Array.empty[URL], baseClassLoader)
  }

  private val defaultThreadFactory = new DynamicDependencyThreadFactory(classLoader = defaultLoader)

  def apply() = defaultThreadFactory

  def addGlobalDependency(urls:Array[java.net.URL]): Unit = {
    urls.foreach(url => defaultLoader.addURL(url) )

  }

  def defaultClassLoader() = defaultLoader

}
