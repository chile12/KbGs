import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.util.zip.GZIPOutputStream

import Main.{Finalize, InsertJoinedSubject, WriterClosed, WriterStart}
import akka.actor.Actor

/**
 * Created by Chile on 8/23/2015.
 */
class JoinedKbCollector() extends Actor {
  var zip: GZIPOutputStream = null
  var writer: BufferedWriter = null
  var counter = 0
  var filename: String = null
  var actor: String = null

  override def receive: Receive =
  {
    case WriterStart(fileName,actor) =>
    {
      this.filename = fileName
      this.actor = actor
      zip = new GZIPOutputStream(new FileOutputStream(new File(fileName)))
      writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"))
      counter = 0
    }
    case InsertJoinedSubject(model) =>
    {
      counter += model.lines.size
      writer.append(model)
    }
    case Finalize() =>
    {
      System.out.println("output file " + filename + " has " + counter + " lines")
      writer.flush()
      writer.close()
      context.parent ! WriterClosed(actor, filename)
    }
    case _ =>
  }
}
