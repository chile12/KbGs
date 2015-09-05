package org.aksw.kbgs.workers

import java.io.StringReader

import akka.actor.{PoisonPill, Actor, ActorRef}
import com.google.common.collect.HashBasedTable
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main
import org.aksw.kbgs.helpers.MultiContextHandler
import org.apache.commons.lang3.StringUtils
import org.openrdf.model.impl.{TreeModel, URIImpl}
import org.openrdf.model.{Literal, URI, Model, Value}
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
      for (j <- 0 until Main.config.kbMap.keys.size) {
        val kb2 = Main.config.kbMap.keys.toParArray.apply(j)
        if(i < j) {  //to make sure we dont compare the same Kb or the same Kbs twice
          fillValueTable(kb1, kb2)
        }
      }
    }
  }
  
  def fillValueTable(kb1: String, kb2: String): Unit =
  {
    val graph1 = Main.config.kbMap.get(kb1).get("kbGraph")
    val graph2 = Main.config.kbMap.get(kb2).get("kbGraph")
    val valueMap = new HashMap[String, Option[Float]]()
    for (prop <- Main.config.properties) {
      val propMap1 = Main.config.getStagePropertyMap(kb1, 0)
      val propMap2 = Main.config.getStagePropertyMap(kb2, 0)
      val uri1 = propMap1.get(prop._1).map(x => if(x.startsWith("http")) new URIImpl(x) else new URIImpl("http://some.org/alibi/uri"))
      val uri2 = propMap2.get(prop._1).map(x => if(x.startsWith("http")) new URIImpl(x) else new URIImpl("http://some.org/alibi/uri"))
      val valSelector1 = modelMap.get(graph1).get.filter(null, uri1.getOrElse(null), null).objects()
      val valSelector2 = modelMap.get(graph2).get.filter(null, uri2.getOrElse(null), null).objects()
      var result =0f
      for(val1 <-valSelector1.toArray(new Array[Value](valSelector1.size())))
        for(val2 <-valSelector2.toArray(new Array[Value](valSelector2.size())))
        {
          if(val1.isInstanceOf[URI] && val2.isInstanceOf[URI])
            result = getEditDistance(val1.stringValue(), val2.stringValue())
          else if(val1.isInstanceOf[Literal] && val2.isInstanceOf[Literal])
          {
            //TODO destinguish between languges and datatypes!
            result = getEditDistance(val1.asInstanceOf[Literal].getLabel, val2.asInstanceOf[Literal].getLabel)
          }
          else
            1+1
        }
      valueMap.put(prop._1, Option(result))
    }
    compTable.put(kb1, kb2, valueMap)
  }

  def getEditDistance(val1: String, val2: String): Float = {
    val dist = StringUtils.getLevenshteinDistance(val1.toLowerCase(), val2.toLowerCase())
    val result = 1f - (dist.asInstanceOf[Float] / Math.max(val1.length, val2.length))
    if (result < 0.333f)
      return 0f
    result
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
              writer ! AddCompResult(kb1, kb2, prop._1, getResultFor(kb1, kb2, prop._1))
            }
            catch {
              case  e: Exception=>
               System.out.println("no pred uri!")
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
      valueMap.get(property).map(x => return (x, 1))
    }

    evalPropTable(valueMap)
  }


  private def evalPropTable(compTable: HashMap[String, Option[Float]]): (Option[Float], Int) = {
    var all = 0
    var hit = 0f
    for (prop <- compTable)
    {
      val zw = prop._2.getOrElse(0f)
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
      val zw = work.asInstanceOf[Option[StringBuilder]]
      zw.map(x => sendResults(x.toString()))
    }
    case Finalize =>
    {
      boss ! Finished(None)
      self ! PoisonPill
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
