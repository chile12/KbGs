package org.aksw.kbgs.workers

import akka.actor.{PoisonPill, Actor, ActorRef}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by Chile on 9/4/2015.
 */
class KbPathFilter(kbPrefix: String, writer: ActorRef, idBuffer : HashMultimap[String, String], propertyMap: mutable.HashMap[String, String]) extends Actor {
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val objectIsUri = HashMultimap.create[String, String]()
  private var boss: ActorRef = null

  def doWork(input: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val l = input.lines.toIterator.next()
      getVals(l) match {
        case Some(thisIds) => {
          for (line <- input.lines) {
            if(hasPredicat(line, thisIds(0))) {
              sb.append(getNewLine(line))
            }
          }
          writer ! InsertJoinedSubject(sb)
        }
        case None =>
      }
    boss ! GimmeWork()
  }

  private def getVals(line: String): Option[List[String]] =
  {
    val subject = line.substring(0, line.indexOf('>')+1)
    if(idBuffer.keySet.contains(subject))
      return Option(idBuffer.get(subject).asScala.toList)
    None
  }

  private def hasPredicat(line: String, lastPred: String): Boolean =
  {
    val startInd = line.indexOf('>')+1
    val pred = line.substring(startInd, line.indexOf('>', startInd)+1).trim
    propertyMap.get(lastPred) match {
      case Some(x) => if(x == pred) true else false  //only if previous predicates (the one which lead aus to this subject) do coincide
      case None => false
    }
  }

  private def getNewLine(line:String): String =
  {
    val idx = line.lastIndexOf('.')
    val triple = line.substring(0, idx)
    val startInd = line.indexOf('>')+1
    val pred = line.substring(startInd, line.indexOf('>', startInd)+1).trim
    objectIsUri.put(triple.substring(triple.lastIndexOf('<'), triple.lastIndexOf('>')+1), pred)
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
      context.actorSelection("/user/distributor") ! Finished(Option((kbPrefix, (null, null, objectIsUri))))
      self ! PoisonPill
    }
    case _ =>
  }
}
