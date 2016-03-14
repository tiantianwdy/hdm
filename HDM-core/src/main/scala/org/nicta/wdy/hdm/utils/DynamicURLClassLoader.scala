package org.nicta.wdy.hdm.utils

import java.net.{URL, URLClassLoader}

/**
 * An editable URL ClassLoader which expose the `addURL` method
 * Created by tiantian on 8/03/16.
 */
private[hdm] class DynamicURLClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent){

  override def addURL(url: URL): Unit = super.addURL(url)

  override def getURLs: Array[URL] = super.getURLs


}
