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

    val repOpt = tempUriMap.get(subject)
    repOpt match {
      case Some(replacement) => {
        for (line <- isb.lines)
        {
          if(replacement == null)
            replacement.replace(" ", "")
          val replaced = replaceSubjectUri(line, subject, replacement)
          replaced match {
            case Some(zw) => sb.append(zw.replace("\n", "") + "\n")
            case None => //sb.append(line.replace("\n", "") + "\n")
          }
        }
        writer ! InsertJoinedSubject(sb)
      }
      case None =>
    }
    boss ! GimmeWork()
  }

  private def replaceSubjectUri(line: String, subject: String, replacement: String): Option[String] =
  {
    var l = line.replace(subject, replacement)
    if(l.contains(newUriStump))
    {
      return None
/*      val ind = l.indexOf(newUriStump)
      val uri = l.substring(ind, l.indexOf('>', ind)+1)
      val zw = tempUriMap.get(uri)
      zw match {
        case Some(x) => l = line.replace(uri, x)
        case None =>
      }*/
    }
    Option(l)
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
      boss ! Finished(None)
      self ! PoisonPill
    }
    case _ =>
  }
}
