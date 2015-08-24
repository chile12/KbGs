import Main.{Finished, InsertJoinedSubject, StartReader}
import akka.actor.{Actor, ActorRef, Props}

import scala.util.control.Breaks._

/**
 * Created by Chile on 8/23/2015.
 */
class KbReader(outputWriter: ActorRef, kbPrefix: String) extends Actor{
  private val source = Main.getSource(Main.config.kbMap.get(kbPrefix).get("kbInput"))
  private val ids = new ConcurrentIdBuffer(Main.getSource(Main.config.kbMap.get(kbPrefix).get("idFile")), 20)
  private var lastRead: String = null
  private var finished = false
  private val workers = context.actorOf(Props(classOf[InstanceEvaluator], outputWriter, kbPrefix).withRouter(new akka.routing.SmallestMailboxRouter(4)))
  private val idProperty = Main.config.kbMap.get(kbPrefix).get("idProperty")
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val newUriStump = "<http://aksw.org/kbgs/isbn/%s>"

  private def readAndSend(): Unit =
  {
    read()
    breakable {
      while(!finished && ids.size > 0)
        readSubject()
      }

    context.parent ! Finished(kbPrefix)
  }

  /**
   * creates and sends the full instance (with all (mapped) properties, defined by the uri to the outputWriter
   * @return uri of the completed instance
   */
  private def readSubject(): Unit=
  {
    while(source.hasNext)
    {
      val uri = evaluateInstance(readNextSubject)
      if(uri != null) {
        ids.remove(uri)
        return
      }
    }
  }

  private def readNextSubject: StringBuilder =
  {
    val sb = new StringBuilder()
    val subject = lastRead.substring(0, lastRead.indexOf(">")+1)
    sb.append(lastRead)
    while (read().startsWith(subject)) {
      sb.append(lastRead)
    }
    sb
  }

  private def read(): String =
  {
    if(source.hasNext)
      lastRead = source.next().trim + "\n"
    else {
      finished = true
      return ""
    }
    lastRead
  }

  override def receive: Receive =
  {
    case StartReader() =>
      readAndSend()
    case _ =>
  }

  def evaluateInstance(input: StringBuilder): String = {
    val sb = new StringBuilder()
    val isbnIdx = input.lines.indexWhere(x => x.contains(idProperty))
    if (isbnIdx >= 0)
    {
      val l = input.lines.toList(isbnIdx)
      val keys = ids.getKeys
      val ind = doesLineContainOneOfThoseUris(l, keys)
      if(ind >= 0) {
        val uri = keys(ind)
        val thisIds = ids.getValues(uri)
        val sameAs = String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + uri + "\t<" + graphName + "> .\n"
        sb.append(sameAs)
        for (line <- input.lines) {
          sb.append(getNewLine(line, uri, thisIds(0)))
        }
        if(thisIds.size > 1)
        {
          for(i <- 1 until thisIds.size)
            sb.append(String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + String.format(newUriStump, thisIds(i)) + "\t<" + graphName + "> .\n")
        }
        outputWriter ! InsertJoinedSubject(sb)
        return uri
      }
    }
    null
  }

  private def doesLineContainOneOfThoseUris(line: String, uriids: List[String] ): Int=
  {
    for(i <- 0 until uriids.size)
      if(line.contains(uriids(i)))
        return i
    -1
  }

  private def getNewLine(line:String, uri:String, id:String): String =
  {
    val zw = line.replace(uri, String.format(newUriStump, id))
    val idx = Math.max(zw.lastIndexOf('>'), zw.lastIndexOf('"'))
    zw.substring(0, idx+1) + "\t<" + graphName + "> .\n"
  }
}
