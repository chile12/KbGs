import Main.{InsertJoinedSubject}
import akka.actor.{Actor, ActorRef}

/**
 * Created by Chile on 8/24/2015.
 */
class InstanceEvaluator(outputWriter: ActorRef, kbPrefix: String) extends Actor{

  private val idProperty = Main.config.kbMap.get(kbPrefix).get("idProperty")
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val newUriStump = "<http://aksw.org/kbgs/" + kbPrefix + "/%s>"

  def evaluateInstance(input: StringBuilder, idBuffer: ConcurrentIdBuffer): String = {
    val sb = new StringBuilder()
    val isbnIdx = input.lines.indexWhere(x => x.contains(idProperty))
    if (isbnIdx >= 0)
    {
      val l = input.lines.toList(isbnIdx)
      val keys = idBuffer.getKeys
      val ind = doesLineContainOneOfThoseUris(l, keys)
      if(ind >= 0) {
        val uri = keys(ind)
        val thisIds = idBuffer.getValues(uri)
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

  override def receive: Receive =
  {
/*    case EvalRequest(input: StringBuilder, idBuffer: ConcurrentIdBuffer) =>
    {
      val uri = evaluateInstance(input, idBuffer)
      if(uri != null)
        idBuffer.remove(uri)
    }*/
    case _ =>
  }
}
