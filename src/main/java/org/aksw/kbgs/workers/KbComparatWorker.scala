package org.aksw.kbgs.workers

import java.io.StringReader

import akka.actor.{Actor, ActorRef}
import com.google.common.collect.HashBasedTable
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main
import org.aksw.kbgs.helpers.MultiContextHandler
import org.openrdf.model.impl.{TreeModel, URIImpl}
import org.openrdf.model.{Model, Value}
import org.openrdf.query.algebra.evaluation.util.ValueComparator
import org.openrdf.rio.RDFParseException
import org.openrdf.rio.nquads.NQuadsParser

import scala.collection.mutable.HashMap

/**
 * Created by Chile on 8/26/2015.
 */
class KbComparatWorker() extends Actor{

  var modelMap: HashMap[String, Model] = null
  var compTable: HashBasedTable[String, String, HashMap[String, Option[Float]]] = null
  var boss: ActorRef = null
  var writer: ActorRef = null
  val comparator = new ValueComparator()


  def doCompare(instance: String): Unit = {
    if(instance == null)
      Main.logger.warning("Could not send eval result due to missing input. Actor: " + self.path)
    modelMap = KbComparatWorker.fillModelMap()
    compTable = HashBasedTable.create[String, String, HashMap[String, Option[Float]]]()
    val sr = new StringReader(instance)
    val parser = new NQuadsParser()
    parser.setRDFHandler(new MultiContextHandler(modelMap))
    try {
      parser.parse(sr, "<http://aksw.org/kbgs/isbn/>")
    }
    catch {
      case e:RDFParseException =>
    }

    for (i <- 0 until  Main.config.kbMap.keys.size) {
      val kb1 = Main.config.kbMap.keys.toParArray.apply(i)
      val graph1 = Main.config.kbMap.get(kb1).get("kbGraph")
      for (j <- 0 until Main.config.kbMap.keys.size) {
        val kb2 = Main.config.kbMap.keys.toParArray.apply(j)
        val graph2 = Main.config.kbMap.get(kb2).get("kbGraph")
        if(i < j) {  //to make sure we dont compare the same Kb or the same Kbs twice
          val valueMap = new HashMap[String, Option[Float]]()
          for (prop <- Main.config.properties) {
            val uri1 = prop._2.get(kb1)
            val uri2 = prop._2.get(kb2)
            if(uri1 != None && uri1.get.length > 3 && uri2 != None && uri2.get.length > 3) {
              val valSelector1 = modelMap.get(graph1).get.filter(null, new URIImpl(uri1.get), null).objects()
              val valSelector2 = modelMap.get(graph2).get.filter(null, new URIImpl(uri2.get), null).objects()
              var result =0
              for(val1 <-valSelector1.toArray(new Array[Value](valSelector1.size())))
                for(val2 <-valSelector2.toArray(new Array[Value](valSelector2.size())))
                  if(comparator.compare(val1, val2) == 0)
                    result = 1
              valueMap.put(prop._1, Option(result))
            }
            else
              valueMap.put(prop._1 + " not used", None)
          }
          compTable.put(kb1, kb2, valueMap)
        }
      }
    }
  }

  def sendResults(input: String): Unit =
  {
    doCompare(input)
    for (i <- 0 until  Main.config.kbMap.keys.size) {
      val kb1 = Main.config.kbMap.keys.toParArray.apply(i)
      for (j <- 0 until Main.config.kbMap.keys.size) {
        val kb2 = Main.config.kbMap.keys.toParArray.apply(j)
        if (i < j) {
          for (prop <- Main.config.properties) {
            try {
              new URIImpl(prop._2.get(kb1).get)
              new URIImpl(prop._2.get(kb2).get)
              writer ! AddCompResult(kb1, kb2, prop._1, getResultFor(kb1, kb2, prop._1))
            }
            catch {
              case  e: Exception=>
                1+1
            }
          }
        }
      }
    }
    boss ! GimmeWork()
  }

  def getResultFor(kb1: String, kb2: String, property: String): (Option[Float], Int) = {

    var valueMap = compTable.get(kb1, kb2)
    if(valueMap == null)
      valueMap = compTable.get(kb2, kb1)
    if(property != null) {
      val prop = valueMap.get(property)
      //TODO Option[Float]!!
      if (prop != None)
        return (prop.get, 1)
      else
        return (None, 1)
    }

    evalPropTable(valueMap)
  }


  private def evalPropTable(compTable: HashMap[String, Option[Float]]): (Option[Float], Int) = {
    var all = 0
    var hit = 0f
    for (prop <- compTable)
    {
      val zw = if(prop._2 != None) prop._2.get else 0
      hit += zw
      all += 1
    }
    (Option(hit), all)
  }

  override def receive: Receive =
  {
    case InitializeWorker(inits) =>
    {
        boss = sender()
        writer = inits.head.asInstanceOf[ActorRef]
        writer ! RegistrateNewWriterSource
        boss ! GimmeWork()
    }
    case Work(work) =>
    {
      val zw = work.asInstanceOf[StringBuilder]
      sendResults(zw.toString())
    }
    case Finalize =>
    {
      boss ! Finished
      context.stop(self)
    }
    case _ =>
  }
}

object KbComparatWorker
{
  private def fillModelMap(): HashMap[String, Model] =
  {
    val modelMap: HashMap[String, Model] = new HashMap[String, Model]()
    for(prop <- Main.config.kbMap) {
      if (modelMap.get(prop._2.get("kbGraph").get) == None)
        modelMap.put(prop._2.get("kbGraph").get, new TreeModel())
    }
    modelMap
  }
}
