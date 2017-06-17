package org.hdm.core.io.reader

import java.io._

import org.codehaus.jackson.JsonParser.Feature
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.{JsonEncoding, JsonFactory}



object JacksonUtils {

  val jsonFactory = new JsonFactory()
  jsonFactory.configure(Feature.AUTO_CLOSE_SOURCE, false)
  lazy val objectMapper = new ObjectMapper(jsonFactory)


  def getJSONParser(is: InputStream) = {
    val jsonParser = jsonFactory.createJsonParser(is)
    jsonParser
  }


  def getJSONWriter(os:OutputStream) ={
    val jsonWriter = jsonFactory.createJsonGenerator(os, JsonEncoding.UTF8)
    jsonWriter
  }



}