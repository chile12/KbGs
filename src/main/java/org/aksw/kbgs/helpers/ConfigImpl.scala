package org.aksw.kbgs.helpers

import java.io.File

import akka.actor.Actor.Receive
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable
import scala.io.BufferedSource

/**
 * Created by Chile on 3/18/2015.
 */
class ConfigImpl(path: String) {

  private val file = new File(path)
  private var source: BufferedSource = null
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  private var jsonString: String = if(file.exists()) {source = scala.io.Source.fromFile(file); source.mkString} else path
  source.close()

  jsonString = jsonString.replaceAll("(?m)^[\\s]*#.*$", "")
  private val config : Map[String, Any] = mapper.readValue(jsonString, classOf[Map[String, Any]])
    //JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]

  val propEvalFile = config.get("jsonResultFile").get.toString
  val uriProviderKb = config.get("uriProviderKb").get.toString
  val tempFile = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + ".tmp"
  val unsorted = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + "_unsorted.nq"
  val sorted = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + "_sorted.nq"
  val kbMap = config.get("kbMap").get.asInstanceOf[Map[String, Map[String, String]]]
  val properties = config.get("properties").get.asInstanceOf[Map[String, Map[String, String]]]
  val numberOfThreads = config.get("numberOfThreads").get.asInstanceOf[Int]
  var intersectionMap:  mutable.HashMap[(String, String), KbIntersectionPreparation] = new mutable.HashMap[(String, String), KbIntersectionPreparation]()
  private val keys = kbMap.keySet.toList
  for(i <- 0 until kbMap.size)
    for(j <- 0 until kbMap.size) {
      if (i < j)
        intersectionMap.put((keys(i), keys(j)), new KbIntersectionPreparation(kbMap.get(keys(i)).get("idFile"), kbMap.get(keys(j)).get("idFile")))
    }

  def cleanUp() =
  {
    for(inter <- intersectionMap.values)
      inter.deleteFiles()
  }
}

object ConfigImpl{
  val DefaultReceive = new Receive {
    def apply(any: Any) = {}
    def isDefinedAt(any: Any) = false
  }
}
