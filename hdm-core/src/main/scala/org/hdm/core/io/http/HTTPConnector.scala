package org.hdm.core.io.http

import java.util
import javax.ws.rs.core.Request

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.{HttpEntity, NameValuePair}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.hdm.core.utils.Logging

import scala.reflect.ClassTag

/**
  * Created by wu056 on 9/06/17.
  */
class HTTPConnector extends Logging {

  private val  httpClient =  HttpClients.createDefault();

  def sendGet[T](url:String, parser: HttpEntity => T):T = {
    val req = new HttpGet(url)
    val resp = httpClient.execute(req)
    val entity = resp.getEntity
    val res = parser(entity)
    // do some thing
    EntityUtils.consume(entity)
    res
  }

  def sendPost[T](url:String, params:Seq[(String, String)], parser: HttpEntity => T):T = {
    val req = new HttpPost(url)
    val nvpr = new util.ArrayList[NameValuePair]()
    params.foreach(nv => nvpr.add(new BasicNameValuePair(nv._1, nv._2)))
    req.setEntity(new UrlEncodedFormEntity(nvpr))
    val resp = httpClient.execute(req)
    if(resp.getStatusLine.getStatusCode == 200){
      val entity = resp.getEntity
      val res = parser(entity)
      // do some thing
      EntityUtils.consume(entity)
      res
    } else {
      log.error(s"Http request failed with code ${resp.getStatusLine.getStatusCode}")
      throw (new RuntimeException(s"Http request failed with code ${resp.getStatusLine.getStatusCode}"))
    }
  }

  def postBytes(url:String, contents:Array[Byte]): Int ={
    val req = new HttpPost(url)
    req.setEntity(new ByteArrayEntity(contents))
    val resp = httpClient.execute(req)
    resp.getStatusLine.getStatusCode
  }


  def secureGet(url:String):Any = ???

  def securePost(url:String, params:Seq[(String, String)]):Any = ???

}
