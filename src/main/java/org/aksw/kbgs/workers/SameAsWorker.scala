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
  private val newUriStump = "<http://aksw.org/kbgs/id"

  def doWork(isb: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val firstLine = isb.lines.next()
    val subject = firstLine.substring(0, firstLine.indexOf(">") + 1)

    for (line <- isb.lines)
    {
      replaceSubjectUri(line, subject) match {
        case Some(zw) => sb.append(zw)
      }
    }
    writer ! InsertJoinedSubject(sb)
    boss ! GimmeWork()
  }

  private def replaceSubjectUri(line: String, subject: String): Option[String] =
  {
    var zw = tempUriMap.get(subject)
    var l = line
    zw match {
      case Some(x) => {
        l = line.replace(subject, zw.get)
        if(l.contains(newUriStump))
        {
          val ind = l.indexOf(newUriStump)
          val uri = l.substring(ind, l.indexOf('>', ind)+1)
          zw = tempUriMap.get(uri)
          if( zw != None)
            l = line.replace(uri, zw.get)
        }
        return Option(l)
      }
    }
    None
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
      val zw = work.asInstanceOf[Option[StringBuilder]]
      zw.map(doWork(_))
    }
    case Finalize => {
      boss ! Finished
      self ! PoisonPill
    }
    case _ =>
  }
}
