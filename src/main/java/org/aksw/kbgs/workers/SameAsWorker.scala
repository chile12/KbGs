package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._

/**
 * Created by Chile on 8/29/2015.
 */
class SameAsWorker(idBuffer:HashMultimap[String, String], writer: ActorRef) extends Actor {

  private var boss: ActorRef = null

  def doWork(isb: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val firstLine = isb.lines.next()
    val subject = firstLine.substring(0, firstLine.indexOf(">") + 1)
    val sameAsValues = idBuffer.get(subject)
    if (sameAsValues.size > 0) {
      for (line <- isb.lines) {
        val replaceWith = sameAsValues.iterator().next()
        sb.append(line.replace(subject, replaceWith))
      }
      writer ! InsertJoinedSubject(sb)
    }
    writer ! InsertJoinedSubject(isb)
    boss ! GimmeWork()
  }

  override def receive: Receive =
  {
    case InitializeWorker(inits) =>
    {
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
      context.stop(self)
    }
    case _ =>
  }
}
