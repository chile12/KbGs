import Main._
import akka.actor.{Actor, ActorRef}

/**
 * Created by Chile on 8/23/2015.
 */
class KnowledgeBaseProcessor(tempWriter: ActorRef, kbPrefix: String) extends Actor with InstanceProcessor[String]{
  private val idBuffer = new ConcurrentIdBuffer(Main.getSource(Main.config.kbMap.get(kbPrefix).get("idFile")).getLines(), 20)
  private val idProperty = Main.config.kbMap.get(kbPrefix).get("idProperty")
  private val graphName = Main.config.kbMap.get(kbPrefix).get("kbGraph")
  private val newUriStump = "<http://aksw.org/kbgs/isbn/%s>"
  private var finished = false

  //first pass: evaluate all instances from sorted source file
  override def startProcess(): Unit =
  {
    tempWriter ! WriterStart(getTempWriterName, self.path.name)
    val instanceReader = new InstanceReader[String](Main.config.kbMap.get(kbPrefix).get("kbInput"))
      while(instanceReader.notFinished() && idBuffer.size > 0)
        instanceReader.readSubject(evaluate, action)
    tempWriter ! Finalize()
    context.parent ! Finished(idBuffer)
    finished = true
  }

  override def evaluate(input: StringBuilder): String = {
      val isb: StringBuilder = input.asInstanceOf[StringBuilder]
      val sb = new StringBuilder()
      val isbnIdx = isb.lines.indexWhere(x => x.contains(idProperty))
      if (isbnIdx >= 0) {
        val l = isb.lines.toList(isbnIdx)
        val keys = idBuffer.getKeys
        val ind = doesLineContainOneOfThoseUris(l, keys)
        if (ind >= 0) {
          val uri = keys(ind)
          val thisIds = idBuffer.getValues(uri)
          val sameAs = String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + uri + "\t<" + graphName + "> .\n"
          sb.append(sameAs)
          for (line <- isb.lines) {
            sb.append(getNewLine(line, uri, thisIds(0)))
          }
          if (thisIds.size > 1) {
            for (i <- 1 until thisIds.size) {
              sb.append(String.format(newUriStump, thisIds(0)) + "\t<http://www.w3.org/2002/07/owl#sameAs>\t" + String.format(newUriStump, thisIds(i)) + "\t<" + graphName + "> .\n")
              idBuffer.addSameAs(String.format(newUriStump, thisIds(i)), String.format(newUriStump, thisIds(0)))
            }
          }
          val test = sb.toString()
          tempWriter ! InsertJoinedSubject(sb)
          return uri
        }
      }
    null
  }

  override def action(uri: String): Unit =
  {
    if(uri != null) {
      idBuffer.remove(uri)
      return
    }
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

  override def finish(): Unit =
  {

  }

  private def getTempWriterName(): String =
  {
    Main.config.tempFile.substring(0,
      Main.config.tempFile.lastIndexOf('.')) + "_" + kbPrefix +
      Main.config.tempFile.substring(Main.config.tempFile.lastIndexOf('.'))
  }

  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case FinishProcessor() =>
      finish()
    case _ =>
  }

}
