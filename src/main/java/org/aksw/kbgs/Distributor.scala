package org.aksw.kbgs

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.helpers.ConfigImpl
import org.aksw.kbgs.inout.{CompResultWriter, WriterActor}
import org.aksw.kbgs.processors._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val outputWriter = context.actorOf(Props(classOf[WriterActor]), "output")
  private val evalWriter = context.actorOf(Props(classOf[CompResultWriter]))
  private val kbMap : ParHashMap[String, (ActorRef, ActorRef)] = new mutable.HashMap[String, (ActorRef, ActorRef)]().par
  for(kbSpecs <- Main.config.kbMap)
  {
    val tempWriter = context.actorOf(Props(classOf[WriterActor]), kbSpecs._1 + "temp")
    val actor = if(Main.config.uriProviderKb == kbSpecs._1)
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], tempWriter, kbSpecs._1, true), kbSpecs._1)
    else
      context.actorOf(Props(classOf[KnowledgeBaseProcessor], tempWriter, kbSpecs._1, false), kbSpecs._1)
    kbMap.put(kbSpecs._1, (actor, tempWriter))
  }
  private val sameAsMap = HashMultimap.create[String, String]()
  private val tempUriMap = new mutable.HashMap[String, String]()
  private val tempfiles = ListBuffer[String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor], tempUriMap, outputWriter), "sameas")
  private val evalActor = context.actorOf(Props(classOf[PropertyCompProcessor],context.actorOf(Props(classOf[Contractor[StringBuilder]])), evalWriter), "evalWriter")

  private def startSameAsResolver(): Unit =
  {
    normalizeLinks()
    for(actor <- kbMap.values)
    {
      if(actor._2 != null)
        actor._2 ! PoisonPill
      if(actor._1 != null)
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
      var zw = tempUriMap.get(i.getKey)
      if(zw == None)
        zw = resolveMissingTempUriEntry(i.getKey)
      if(zw != None)
        tempUriMap.put(i.getValue, zw.get)
      else
        true
    }
  }

  private def resolveMissingTempUriEntry(key: String): Option[String] =
  {
    var zw: Option[String] = null
    val set = sameAsMap.get(key).asScala
    for (k <- set) {
      zw = tempUriMap.get(k)
      if (zw != None)
        return zw
    }
    None
  }

  override def receive: Receive =
  {
    case Finished(sam) =>
    {
      val ret = sam.get.asInstanceOf[(HashMultimap[String, String], HashMultimap[String, String])]
      sameAsMap.putAll(ret._1)
      for(ent <- ret._2.entries().asScala)
        tempUriMap.put(ent.getKey, ent.getValue)
    }
    case StartProcess =>
    {
      kbMap.values.map(x => x._1 ! StartProcess())
      //sameAsActor ! StartSameAsActor(List("kbEvalOut.nq.tmp_bnb.gz", "kbEvalOut.nq.tmp_cam.gz"))
      //evalActor ! StartProcess
    }
    case WorkersInitialized() =>

    case WriterClosed(actor, filename) =>
    {
      if(Main.config.propEvalFile == filename)
      {
        Main.config.cleanUp()
        context.system.shutdown()
      }
      if(Main.config.unsorted == filename) {
        sameAsActor ! PoisonPill
        outputWriter ! PoisonPill
        evalActor ! StartProcess
      }
      if(this.kbMap.keySet.contains(actor))
      {
        kbMap.update(actor, (sender, null))
        tempfiles += filename
        for(zz <- kbMap.values) {
          if (zz._2 != null)
            return ConfigImpl.DefaultReceive
        }
        startSameAsResolver
      }
    }
    case _ =>
  }
}
