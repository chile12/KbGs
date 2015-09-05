package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.Main

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by Chile on 8/27/2015.
 */
class KbFilter(writer: ActorRef, kbPrefix: String, idBuffer : HashMultimap[String, String], propertyMap: mutable.HashMap[String, String], isUriProvider: Boolean = false) extends Actor{
  private val idProperty = Main.config.kbMap.get(kbPrefix).get("idProperty")
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val newUriStump = "<http://aksw.org/kbgs/id/%s>"
  private val sameAsMap = HashMultimap.create[String, String]()
  private val tempUriMap = HashMultimap.create[String, String]()
  private val objectIsUri = HashMultimap.create[String, (String, String)]()
  private var boss: ActorRef = null

  def doWork(input: StringBuilder): Unit =
  {
    val sb = new StringBuilder()
    val isbnIdx = input.lines.indexWhere(x => x.contains(idProperty))
    if (isbnIdx >= 0) {
      val l = input.lines.toList(isbnIdx)
      val uri = l.substring(0, l.indexOf('>')+1)
      getVals(l, uri) match {
        case Some(thisIds) => {
          val sameAs = String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + uri + "\t<" + graphName + "> .\n"
          sb.append(sameAs)
          for (line <- input.lines) {
            if(hasPredicat(line)) {
              sb.append(getNewLine(line, uri, thisIds(0)))
            }
          }
          if (thisIds.size > 1) {
            for (i <- 1 until thisIds.size) {
              sb.append(String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + String.format(newUriStump, thisIds(i)) + "\t<" + graphName + "> .\n")
              sameAsMap.put(String.format(newUriStump, thisIds(i)), String.format(newUriStump, thisIds(0)))
            }
          }
          writer ! InsertJoinedSubject(sb)
          if (isUriProvider)
            tempUriMap.put(String.format(newUriStump, thisIds(0)), uri)
        }
        case None =>
      }
    }
    boss ! GimmeWork()
  }

  private def getVals(line: String, subject: String): Option[List[String]] =
  {

    if(idBuffer.keySet.contains(subject))
      return Option(idBuffer.get(subject).asScala.toList)
    None
  }

  private def hasPredicat(line: String): Boolean =
  {
    val startInd = line.indexOf('>')+1
    val pred = line.substring(startInd, line.indexOf('>', startInd)).trim.replace("<", "")
    propertyMap.valuesIterator.contains(pred)
  }

  private def getNewLine(line:String, uri:String, id:String): String =
  {
    val zw = line.replace(uri, String.format(newUriStump, id))
    val idx = zw.lastIndexOf('.')
    val triple = zw.substring(0, idx)
    if(!(triple.contains("^^") || triple.contains("\"@") || triple.lastIndexOf('>') < triple.lastIndexOf('"'))) //not!,  object is uri
    {

      val startInd = line.indexOf('>')+1
      val pred = line.substring(startInd, line.indexOf('>', startInd)+1).trim
      val obj = triple.substring(triple.lastIndexOf('<'), triple.lastIndexOf('>')+1).trim
      objectIsUri.put(obj, (pred, obj))
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
      boss ! Finished(Option((kbPrefix, (Option(sameAsMap), Option(tempUriMap), Option(objectIsUri)))))
      self ! PoisonPill
    }
    case _ =>
  }
}
