package org.hdm.core.utils

import java.net.{URL, URLClassLoader}

/**
 * An editable URL ClassLoader which expose the `addURL` method
 * Created by tiantian on 8/03/16.
 */
class DynamicURLClassLoader(val urls: Array[URL], val parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = super.addURL(url)

  override def getURLs: Array[URL] = super.getURLs


}
