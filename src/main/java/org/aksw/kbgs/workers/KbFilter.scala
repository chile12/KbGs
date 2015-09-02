package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main

import scala.collection.mutable

/**
 * Created by Chile on 8/27/2015.
 */
class KbFilter(writer: ActorRef, kbPrefix: String, idBuffer : mutable.HashMap[String, mutable.Set[String]]) extends Actor{
  private val idProperty = Main.config.kbMap.get(kbPrefix).get("idProperty")
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val newUriStump = "<http://aksw.org/kbgs/isbn/%s>"
  private val sameAsMap = HashMultimap.create[String, String]()
  private var boss: ActorRef = null
  private var idBufferActor: ActorRef = null

  def doWork(input: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val lines = input.lines
    val isbnIdx = input.lines.indexWhere(x => x.contains(idProperty))
    if (isbnIdx >= 0) {
      val l = input.lines.toList(isbnIdx)
      val uri = getSubject(l)
      if (uri != null) {
        var thisIds = idBuffer.get(uri).get.toList
        val sameAs = String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + uri + "\t<" + graphName + "> .\n"
        sb.append(sameAs)
        for (line <- input.lines) {
          sb.append(getNewLine(line, uri, thisIds(0)))
        }
        if (thisIds.size > 1) {
          for (i <- 1 until thisIds.size) {
            sb.append(String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + String.format(newUriStump, thisIds(i)) + "\t<" + graphName + "> .\n")
            sameAsMap.put(String.format(newUriStump, thisIds(i)), String.format(newUriStump, thisIds(0)))
          }
        }
        writer ! InsertJoinedSubject(sb)
      }
    }
    boss ! GimmeWork()
  }

  private def getSubject(line: String): String=
  {
    val subject = line.substring(0, line.indexOf('>')+1)
    if(idBuffer.keySet.contains(subject))
      return subject
    null
  }

  private def getNewLine(line:String, uri:String, id:String): String =
  {
    val zw = line.replace(uri, String.format(newUriStump, id))
    val idx = Math.max(zw.lastIndexOf('>'), zw.lastIndexOf('"'))
    zw.substring(0, idx+1) + "\t<" + graphName + "> .\n"
  }

  override def receive: Receive = {
    case InitializeWorker(inits) =>
    {
        writer ! RegistrateNewWriterSource
        boss ! GimmeWork()
    }
    case Work(work) =>
    {
      val zw = work.asInstanceOf[StringBuilder]
      doWork(zw)
    }
    case AssignWorkers(b) =>
    {
      boss = b
    }
    case Finalize => {
      boss ! Finished
      context.actorSelection("/user/distributor") ! Finished(Option(sameAsMap))
      context.stop(self)
    }
    case _ =>
  }
}
