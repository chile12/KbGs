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
    val actor = context.actorOf(Props(classOf[KnowledgeBaseProcessor], tempWriter, kbSpecs._1), kbSpecs._1)
    kbMap.put(kbSpecs._1, (actor, tempWriter))
  }
  private val sameAsMap = HashMultimap.create[String, String]()
  private val tempfiles = ListBuffer[String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor],context.actorOf(Props(classOf[Contractor[StringBuilder]])), sameAsMap, outputWriter), "sameas")
  private val evalActor = context.actorOf(Props(classOf[PropertyCompProcessor],context.actorOf(Props(classOf[Contractor[StringBuilder]])), evalWriter), "evalWriter")

  private def startSameAsResolver(): Unit =
  {
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

  private def normalizeSameAsLinks() {
    val removeList = ListBuffer[(String, String)]()
    val entries = sameAsMap.entries().asScala
    for (i <- entries) {
      if (sameAsMap.keySet.contains(i.getValue))
        removeList += ((i.getKey, i.getValue))
    }
    for (entry <- removeList)
      sameAsMap.remove(entry._1, entry._2)
  }

  override def receive: Receive =
  {
    case Finished(sam) =>
    {
      sameAsMap.putAll(sam.get.asInstanceOf[HashMultimap[String, String]])
      normalizeSameAsLinks()
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
        context.system.shutdown()
      if(Main.config.outFile == filename) {
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
