package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by Chile on 9/4/2015.
 */
class KbPathFilter(kbPrefix: String, idBuffer : HashMultimap[String, (String, String)], propertyMap: mutable.HashMap[String, String]) extends Actor {
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val objectIsUri = HashMultimap.create[String, (String, String)]()
  private var boss: ActorRef = null

  def doWork(input: StringBuilder): Unit =
  {
    val l = input.lines.toIterator.next()
      getVals(l) match {
        case Some(thisIds) => {
          for (line <- input.lines) {
            if(hasPredicat(line, thisIds(0)._1)) {
              resolveUriObject(line)
            }
          }
        }
        case None =>
      }
    boss ! GimmeWork()
  }

  private def getVals(line: String): Option[List[(String, String)]] =
  {
    val subject = line.substring(0, line.indexOf('>')+1)
    if(idBuffer.keySet.contains(subject))
      return Option(idBuffer.get(subject).asScala.toList)
    None
  }

  private def hasPredicat(line: String, lastPred: String): Boolean =
  {
    val startInd = line.indexOf('>')+1
    val pred = line.substring(startInd, line.indexOf('>', startInd)).trim.replace("<", "")
    propertyMap.get(lastPred.replace("<", "").replace(">", "")) match {
      case Some(x) => if(x == pred) true else false  //only if previous predicates (the one which lead aus to this subject) do coincide
      case None => false
    }
  }

  private def resolveUriObject(line:String): Unit =
  {
    val idx = line.lastIndexOf('.')
    val triple = line.substring(0, idx+1)
    var startInd = line.indexOf('>')+1
    val subj = line.substring(0, startInd).trim
    val pred = line.substring(startInd, line.indexOf('>', startInd)+1).trim
    startInd = line.indexOf('>', startInd)+1
    val obj = line.substring(startInd, line.lastIndexOf('.')).trim
    objectIsUri.put(subj, (pred, obj))
  }

  override def receive: Receive = {
    case InitializeWorker(inits) =>
    {
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
      boss ! Finished(Option((kbPrefix, (None, None, Option(objectIsUri)))))
      self ! PoisonPill
    }
    case _ =>
  }
}
