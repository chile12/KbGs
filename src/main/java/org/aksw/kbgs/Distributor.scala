package org.aksw.kbgs

import java.util

import akka.actor.{PoisonPill, Actor, ActorRef, Props}
import org.aksw.kbgs.Main._
import org.aksw.kbgs.helpers.{ConcurrentIdBuffer, ConfigImpl}
import org.aksw.kbgs.inout.{CompResultWriter, GzWriterActor}
import org.aksw.kbgs.processors._

import scala.collection.mutable
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val outputWriter = context.actorOf(Props(classOf[GzWriterActor]), "output")
  private val evalWriter = context.actorOf(Props(classOf[CompResultWriter]))
  private val strawBoss = context.actorOf(Props(classOf[StrawBoss]))
  private val kbMap : ParHashMap[String, (ActorRef, ActorRef)] = new mutable.HashMap[String, (ActorRef, ActorRef)]().par
  for(kbSpecs <- Main.config.kbMap)
  {
    val tempWriter = context.actorOf(Props(classOf[GzWriterActor]), kbSpecs._1 + "temp")
    val actor = context.actorOf(Props(classOf[KnowledgeBaseProcessor], tempWriter, kbSpecs._1), kbSpecs._1)
    kbMap.put(kbSpecs._1, (actor, tempWriter))
  }
  private var centralSameAsBuffer: ConcurrentIdBuffer = null
  private val tempfiles: util.ArrayList[String] = new util.ArrayList[String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor],outputWriter), "sameas")
  private val evalActor = context.actorOf(Props(classOf[PropertyCompProcessor],strawBoss, evalWriter), "evalWriter")

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
      sameAsActor ! StartSameAsActor(tempfiles.toArray(new Array[String](tempfiles.size)), centralSameAsBuffer)
    else
      Main.logger.severe("sameAs actor was not initialized yet!")
  }

  override def receive: Receive =
  {
    case Finished(idBuffer) =>
    {
      if(centralSameAsBuffer == null)
        centralSameAsBuffer = idBuffer
      else
        centralSameAsBuffer.addSameAsBuffer(idBuffer)
    }
    case StartProcess =>
    {
      kbMap.values.map(x => x._1 ! StartProcess())
      //evalActor ! StartProcess
    }
    case WorkersInitialized(id) =>

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
        tempfiles.add(filename)
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
