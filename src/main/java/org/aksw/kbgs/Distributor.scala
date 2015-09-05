package org.aksw.kbgs

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.processors._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val kbMap : ParHashMap[String, ActorRef] = new mutable.HashMap[String, ActorRef]().par
  private val sameAsMap = HashMultimap.create[String, String]()
  private val tempUriMap = new mutable.HashMap[String, String]()
  private var objectIsUri= new mutable.HashMap[String, HashMultimap[String, (String, String)]]()
  private var globalUriToValueMap= new mutable.HashMap[String, String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor], tempUriMap), "sameas")
  private val evalActor = context.actorOf(Props(classOf[PropertyCompProcessor]), "evalWriter")

  for(kbSpecs <- Main.config.kbMap)
  {
    Main.config.createPropertyMaps(kbSpecs._1)
    val propMap = Main.config.getStagePropertyMap(kbSpecs._1, 0)
    val actor = if(Main.config.uriProviderKb == kbSpecs._1)
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], kbSpecs._1, propMap, true), kbSpecs._1)
    else
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], kbSpecs._1, propMap, false), kbSpecs._1)
    kbMap.put(kbSpecs._1, actor)
    objectIsUri.put(kbSpecs._1, HashMultimap.create[String, (String, String)]())
  }

  private def startSameAsResolver(): Unit =
  {
    normalizeLinks()
    for(actor <- kbMap.values)
    {
      if(actor != null)
        actor ! PoisonPill
    }
    if(sameAsActor != null)
      sameAsActor ! StartSameAsActor(Main.config.getTempFiles().values.toList)
    else
      Main.logger.severe("sameAs actor was not initialized yet!")
  }

  private def normalizeLinks()= {
    val removeList = ListBuffer[(String, String)]()
    for (i <- sameAsMap.entries().asScala) {
      if (sameAsMap.keySet.contains(i.getValue))
        removeList += ((i.getKey, i.getValue))
    }
    for (entry <- removeList)
      sameAsMap.remove(entry._1, entry._2)
    removeList.clear()
    for (i <- sameAsMap.entries().asScala) {
      val zw = resolveMissingTempUriEntry(i.getKey)
      match{
        case Some(x) => tempUriMap.put(i.getValue, x)
        case None =>
      }
    }
  }

  private def resolveMissingTempUriEntry(key: String): Option[String] =
  {
    var zw:Option[String] = tempUriMap.get(key)
    zw match {
      case Some(x) => return Option(x)
      case None => {
        val set = sameAsMap.get(key).asScala
        for (k <- set) {
          zw = tempUriMap.get(k)
          zw match {
            case Some(x) => return Option(x)
            case None =>
          }
        }
      }
    }
    None
  }

  private def dissolveUrisToValue(objectMap: HashMultimap[String, (String, String)]): Unit =
  {
    for(key <- objectMap.keySet().asScala)
    {
      if(!globalUriToValueMap.valuesIterator.contains(key)) //not!
        objectMap.get(key).asScala.map(x => globalUriToValueMap.put(key, x._2))
      else
      {
        var tt = ""
        globalUriToValueMap.iterator.indexWhere(x => {
          tt = x._1
          (x._2 == key)
        })
        objectMap.get(key).asScala.map(x => globalUriToValueMap.update(tt, x._2))
      }
    }
  }

  private var writerCloseCounter = 0

  override def receive: Receive =
  {
    case StartProcess =>
    {
      //kbMap.values.map(x => x ! StartProcess())
      //sameAsActor ! StartSameAsActor(List("propEvalOut_tempbnb2.gz", "propEvalOut_tempdbp2.gz"))
      evalActor ! StartProcess
    }
    case Finished(sam) =>
    {
      val ret = sam.map(z => {
        val outer = z.asInstanceOf[(String, (Option[HashMultimap[String, String]], Option[HashMultimap[String, String]], Option[HashMultimap[String, (String, String)]]))]
        outer._2.x._1.map(sameAsMap.putAll(_))
        outer._2.x._2.map(_.entries().asScala.map(u => tempUriMap.put(u.getKey, u.getValue)))
        outer._2.x._3.map(y => objectIsUri.get(outer._1).map(_.putAll(y)))
      })
    }
    case WorkersInitialized() =>
    case ProcessorFinished(kbPrefix: String, stage: Int) =>{
      val nextStage = Main.config.getStagePropertyMap(kbPrefix, stage+1)
      val idBuffer = objectIsUri.get(kbPrefix).getOrElse(HashMultimap.create[String, (String, String)]())
      objectIsUri.put(kbPrefix, HashMultimap.create[String, (String, String)]())
      dissolveUrisToValue(idBuffer)
      if(nextStage.size > 0)
      {
        val stageExtractor = context.actorOf(Props(classOf[PropertyFilterProcessor], kbPrefix, stage+1, idBuffer, nextStage))
        kbMap.put(kbPrefix, stageExtractor)
        stageExtractor ! StartProcess()
      }
      else
      {
        writerCloseCounter += 1
        if(writerCloseCounter == 2)
          startSameAsResolver
      }
    }
    case UriPathsResolved(fileName: String) =>
    {
      evalActor ! StartProcess
    }
    case CompProcFinished() =>
    {
      Main.config.cleanUp()
      context.system.shutdown()
    }
    case SameAsFinished() =>
    {
      val actor = context.actorOf(Props(classOf[ObjectUriProcessor], Main.config.getTempFiles().values.toList, globalUriToValueMap))
      actor ! StartProcess()
    }
    case _ =>
  }
}
