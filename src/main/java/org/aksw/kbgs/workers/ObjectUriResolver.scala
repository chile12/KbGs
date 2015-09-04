package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._

import scala.collection.mutable

/**
 * Created by Chile on 9/4/2015.
 */
class ObjectUriResolver(kbPrefix: String, writer: ActorRef, valueMap: mutable.HashMap[String, String]) extends Actor{
  private val objectIsUri = HashMultimap.create[String, String]()
  private var boss: ActorRef = null

  def doWork(input: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    for (line <- input.lines) {
        sb.append(resolveObjUri(line))
    }
    writer ! InsertJoinedSubject(sb)
    boss ! GimmeWork()
  }

  private def resolveObjUri(line:String): String =
  {
    val idx = line.lastIndexOf('<')
    val graphName = line.substring(idx).trim
    val triple = line.substring(0, idx-1).trim
    if(!(triple.contains("^^") || triple.contains("\"@") || triple.lastIndexOf('>') < triple.lastIndexOf('"'))) //not!,  object is uri
    {
      val startInd = line.lastIndexOf('<')
      val obj = line.substring(startInd).trim
      valueMap.get((obj)) match{
        case Some(value) => return triple.replace(obj, value) + "\t<" + graphName + "> .\n"
        case None =>
      }
    }
    triple + "\t<" + graphName + "> .\n"
  }

  override def receive: Receive = {
    case InitializeWorker(inits) =>
    {
      writer ! RegistrateNewWriterSource
      boss ! GimmeWork()
    }
    case Work(work) =>
    {
      val zw = work.asInstanceOf[Option[StringBuilder]]
      zw.map(doWork(_))
    }
    case AssignWorkers(b) =>
    {
      boss = b
    }
    case Finalize => {
      boss ! Finished
      context.actorSelection("/user/distributor") ! Finished(Option((kbPrefix, (None, None, objectIsUri))))
      self ! PoisonPill
    }
    case _ =>
  }
}
