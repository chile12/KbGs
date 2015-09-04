package org.aksw.kbgs

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.WriterActor
import org.aksw.kbgs.processors._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val kbMap : ParHashMap[String, (ActorRef, ActorRef)] = new mutable.HashMap[String, (ActorRef, ActorRef)]().par
  private val propertyPathMap = new collection.mutable.HashMap[String, HashMultimap[Int, (String, String)]]
  private val sameAsMap = HashMultimap.create[String, String]()
  private val tempUriMap = new mutable.HashMap[String, String]()
  private var objectIsUri= new mutable.HashMap[String, HashMultimap[String, String]]()
  private var globalUriToValueMap= new mutable.HashMap[String, String]()
  private val objectToValue = new mutable.HashMap[String, String]()
  private val tempfiles = ListBuffer[String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor], tempUriMap), "sameas")
  private val evalActor = context.actorOf(Props(classOf[PropertyCompProcessor]), "evalWriter")

  for(kbSpecs <- Main.config.kbMap)
  {
    val propMap = createPropertyMaps(kbSpecs._1)
    val actor = if(Main.config.uriProviderKb == kbSpecs._1)
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], kbSpecs._1, propMap, true), kbSpecs._1)
    else
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], kbSpecs._1, propMap, false), kbSpecs._1)
    val writer = context.actorOf(Props(classOf[WriterActor]), kbSpecs._1 + "objUris")
    writer ! WriterStart(Main.config.tempFile + "_" + kbSpecs._1 + "objUris", kbSpecs._1)
    kbMap.put(kbSpecs._1, (actor, writer))
    objectIsUri.put(kbSpecs._1, HashMultimap.create[String, String]())
  }

  private def startSameAsResolver(): Unit =
  {
    normalizeLinks()
    for(actor <- kbMap.values)
    {
      if(actor != null)
        actor._1 ! PoisonPill
    }
    if(sameAsActor != null)
      sameAsActor ! StartSameAsActor(tempfiles.toList)
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
      tempUriMap.put(i.getValue, zw)
    }
  }

  private def resolveMissingTempUriEntry(key: String): String =
  {
    var zw:Option[String] = tempUriMap.get(key)
    zw match {
      case Some(x) => return x
      case None => {
        val set = sameAsMap.get(key).asScala
        for (k <- set) {
          zw = tempUriMap.get(k)
          zw match {
            case Some(x) => return x
          }
        }
      }
    }
    null
  }

  private def createPropertyMaps(kbPrefix: String): collection.mutable.HashMap[String, String] =
  {
    val map = new collection.mutable.HashMap[String, String]()
    val followUp = HashMultimap.create[Int, (String, String)]()
    for(prop <- Main.config.properties)
      prop._2.get(kbPrefix) match{
        case Some(x) => {
          val pipeSplit = x.replace(" ", "").split("\\|\\|")
          for(opt <- pipeSplit)
          {
            val slashSplit = opt.split("->")
            if(slashSplit(0).trim.length > 0)
              map.put("<" + slashSplit(0) + ">", if(slashSplit.size > 1) slashSplit(1) else "")
            for(i <- 1 until slashSplit.length)
              followUp.put(i, ("<" + slashSplit(i-1) + ">", "<" + slashSplit(i) + ">"))
          }
        }
        case None =>
      }
    propertyPathMap.put(kbPrefix, followUp)
    map
  }

  private def dissolveUrisToValue(objectMap: HashMultimap[String, String]): Unit =
  {
    for(key <- objectToValue.keySet)
    {
      if(!globalUriToValueMap.valuesIterator.contains(key)) //not!
        objectToValue.get(key).map(globalUriToValueMap.put(key,_))
      else
      {
        var tt = ""
        globalUriToValueMap.iterator.indexWhere(x => {
          tt = x._1
          (x._2 == key)
        })
        objectToValue.get(key).map(globalUriToValueMap.update(tt, _))
      }
    }
  }

  private def getNextStage(kbPrefix: String): collection.mutable.HashMap[String, String] =
  {
    var minKey = Int.MaxValue
    val propMap = propertyPathMap.get(kbPrefix).getOrElse(HashMultimap.create[Int, (String, String)]())
    propMap.keySet().asScala.map(x => minKey = Math.min(x, minKey))
    val map = new collection.mutable.HashMap[String, String]()
    propMap.get(minKey).asScala.map(x => map.put(x._1, x._2))
    propMap.removeAll(minKey)
    map
  }

  override def receive: Receive =
  {
    case Finished(sam) =>
    {
      val ret = sam.get.asInstanceOf[(String, (HashMultimap[String, String], HashMultimap[String, String], HashMultimap[String, String]))]
      if(ret._2._1 != null)
        sameAsMap.putAll(ret._2._1)
      if(ret._2._2 != null)
        for(ent <- ret._2._2.entries().asScala)
          tempUriMap.put(ent.getKey, ent.getValue)
      if(ret._2._3 != null)
        objectIsUri.get(ret._1).map(_.putAll(ret._2._3))
    }
    case StartProcess =>
    {
      kbMap.values.map(x => x._1 ! StartProcess())
      //sameAsActor ! StartSameAsActor(List("kbEvalOut.nq.tmp_bnb.gz", "kbEvalOut.nq.tmp_cam.gz"))
      //evalActor ! StartProcess
    }
    case WorkersInitialized() =>
    case ProcessorFinished(kbPrefix: String) =>{
      val nextStage = getNextStage(kbPrefix)
      if(nextStage.size > 0)
      {
        val idBuffer = objectIsUri.get(kbPrefix).getOrElse(HashMultimap.create[String, String]())
        val writer = kbMap.get(kbPrefix).get._2
        val stageExtractor = context.actorOf(Props(classOf[PropertyFilterProcessor], kbPrefix, writer, idBuffer, nextStage))
        kbMap.put(kbPrefix, (stageExtractor, writer))
        stageExtractor ! StartProcess()
        dissolveUrisToValue(idBuffer)
        objectIsUri.put(kbPrefix, HashMultimap.create[String, String]())
      }
      else
      {
        val writer = kbMap.get(kbPrefix).get._2
        writer ! Finalize
        objectIsUri.put(kbPrefix, null)
        kbMap.put(kbPrefix, null)

      }
    }
    case UriPathsResolved(kbPrefix: String, fileName: String) =>
    {
      if(this.kbMap.keySet.contains(kbPrefix)) {
        kbMap.update(kbPrefix, null)
        tempfiles += fileName
        if(tempfiles.length == Main.config.numberOfThreads)
          startSameAsResolver
      }
    }
    case WriterClosed(actor, filename) =>
    {
      if(Main.config.propEvalFile == filename)
      {
        Main.config.cleanUp()
        context.system.shutdown()
      }
      if(Main.config.unsorted == filename) {
        sameAsActor ! PoisonPill
        evalActor ! StartProcess
      }
    }
    case _ =>
  }
}
