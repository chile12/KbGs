package org.aksw.kbgs.helpers

import java.io.File

import akka.actor.Actor.Receive
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Main

import scala.collection.JavaConverters._
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

  private val tempfiles = new mutable.HashMap[(String, String), mutable.HashMap[String, String]]()
  jsonString = jsonString.replaceAll("(?m)^[\\s]*#.*$", "")
  private val config : Map[String, Any] = mapper.readValue(jsonString, classOf[Map[String, Any]])
    //JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]

  val propEvalFile = config.get("jsonResultFile").get.toString
  val uriProviderKb = config.get("uriProviderKb").get.toString
  val tempFile = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + "x_temp.gz"
  val unsorted = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + "x_unsorted.nq"
  val sorted = propEvalFile.substring(0, propEvalFile.lastIndexOf('.')) + "x_sorted.nq"
  val kbMap = config.get("kbMap").get.asInstanceOf[Map[String, Map[String, String]]]
  val properties = config.get("properties").get.asInstanceOf[Map[String, Map[String, String]]]
  val numberOfThreads = config.get("numberOfThreads").get.asInstanceOf[Int]
  val propertyPathMap = new collection.mutable.HashMap[String, HashMultimap[Int, (String, String)]]
  //TODO matchups
  var currentMatchUp = (kbMap.keys.toList(0), kbMap.keys.toList(1))
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

  def getTempFiles(): mutable.HashMap[String, String] = {
    var temp: mutable.HashMap[String, String] = null
    tempfiles.get(currentMatchUp) match {
      case Some(x) => temp = x
      case None => tempfiles.get((currentMatchUp._2, currentMatchUp._1)) match {
        case Some(y) => temp = y
        case None =>
      }
    }
    if (temp == null) {
      temp = new mutable.HashMap[String, String]()
      tempfiles.put(currentMatchUp, temp)
    }
    temp
  }

  def getTempFile(kbPrefix: String, pass: Int, save: Boolean = false): String =
  {
    val temp: mutable.HashMap[String, String] = getTempFiles()
    val ret = tempFile.substring(0, tempFile.lastIndexOf('.')) + kbPrefix + pass + ".gz"
    if(save)
      temp.put(kbPrefix, ret)
    ret
  }


  def createPropertyMaps(kbPrefix: String): Unit =
  {
    val followUp = HashMultimap.create[Int, (String, String)]()
    for(prop <- properties)
      prop._2.get(kbPrefix) match{
        case Some(x) => {
          val pipeSplit = x.replace(" ", "").split("\\|\\|")
          for(opt <- pipeSplit)
          {
            val slashSplit = opt.split("->")
            for(i <- 0 until slashSplit.length)
              followUp.put(i, (if(i > 0) slashSplit(i-1) else prop._1, slashSplit(i)))
          }
        }
        case None =>
      }
    propertyPathMap.put(kbPrefix, followUp)
  }

  def getStagePropertyMap(kbPrefix: String, stage: Int): collection.mutable.HashMap[String, String] =
  {
    val propMap = Main.config.propertyPathMap.get(kbPrefix).getOrElse(HashMultimap.create[Int, (String, String)]())
    val map = new collection.mutable.HashMap[String, String]()
    propMap.get(stage).asScala.map(x => map.put(x._1, x._2))
    map
  }
}

object ConfigImpl{
  val DefaultReceive = new Receive {
    def apply(any: Any) = {}
    def isDefinedAt(any: Any) = false
  }

}
