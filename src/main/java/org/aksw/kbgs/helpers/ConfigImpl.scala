package org.aksw.kbgs.helpers

import java.io.File

import akka.actor.Actor.Receive
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.io.BufferedSource

/**
 * Created by Chile on 3/18/2015.
 */
class ConfigImpl(path: String) {

  private val file = new File(path)
  private var source: BufferedSource = null
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  private val jsonString: String = if(file.exists()) {source = scala.io.Source.fromFile(file); source.mkString} else path
  source.close()

  private val config : Map[String, Any] = mapper.readValue(jsonString, classOf[Map[String, Any]])
    //JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]

  val kbMap = config.get("kbMap").get.asInstanceOf[Map[String, Map[String, String]]]
  val outFile = config.get("outFile").get.toString
  val propEvalFile = config.get("propEval").get.toString
  val tempFile = outFile.substring(0, outFile.lastIndexOf('.')) + ".tmp" + outFile.substring(outFile.lastIndexOf('.'))
  val properties = config.get("properties").get.asInstanceOf[Map[String, Map[String, String]]]
  val sortMemUsage = config.get("sortMemUsage").get.asInstanceOf[Int]
}

object ConfigImpl{
  val DefaultReceive = new Receive {
    def apply(any: Any) = {}
    def isDefinedAt(any: Any) = false
  }
}
