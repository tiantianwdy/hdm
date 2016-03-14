package org.nicta.wdy.hdm.executor

import java.net.URLClassLoader
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ThreadFactory}

import org.nicta.wdy.hdm.utils.DynamicURLClassLoader

/**
 * Created by tiantian on 7/03/16.
 */
class DynamicDependencyThreadFactory(private val under:ThreadFactory = Executors.defaultThreadFactory(),
                                     private val classLoader:DynamicURLClassLoader) extends ThreadFactory {

  val baseLoader = new AtomicReference[ClassLoader](classLoader)

  def addURLs(urls:Array[java.net.URL]): Unit ={
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
    new DynamicURLClassLoader(Array.empty[URL], ClassLoader.getSystemClassLoader)
  }

  private val defaultThreadFactory = new DynamicDependencyThreadFactory(classLoader = defaultLoader)

  def apply() = defaultThreadFactory

  def addGlobalDependency(urls:Array[java.net.URL]): Unit = {
    urls.foreach(url => defaultLoader.addURL(url) )

  }

  def defaultClassLoader() = defaultLoader

}
