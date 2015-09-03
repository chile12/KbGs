package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef, PoisonPill}
import org.aksw.kbgs.Contractor._

import scala.collection.mutable


/**
 * Created by Chile on 8/29/2015.
 */
class SameAsWorker(writer: ActorRef) extends Actor {

  private var boss: ActorRef = null
  private var tempUriMap: mutable.HashMap[String, String] =null

  def doWork(isb: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val firstLine = isb.lines.next()
    val subject = firstLine.substring(0, firstLine.indexOf(">") + 1)

    for (line <- isb.lines)
    {
      val zw = replaceSubjectUri(line, subject)
      if( zw != null)
        sb.append(zw)
    }
    writer ! InsertJoinedSubject(sb)
    boss ! GimmeWork()
  }

  private def replaceSubjectUri(line: String, subject: String): String =
  {
    val zw = tempUriMap.get(subject)
    if( zw != None)
      line.replace(subject, zw.get)
    else
      null
  }

  override def receive: Receive =
  {
    case InitializeWorker(inits) =>
    {
      tempUriMap = inits(0).asInstanceOf[mutable.HashMap[String, String]]
      boss = sender()
      writer ! RegistrateNewWriterSource
      boss ! GimmeWork()
    }
    case Work(work) =>
    {
      val zw = work.asInstanceOf[StringBuilder]
      doWork(zw)
    }
    case Finalize => {
      boss ! Finished
      self ! PoisonPill
    }
    case _ =>
  }
}
