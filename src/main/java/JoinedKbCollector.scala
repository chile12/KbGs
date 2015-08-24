import java.io.{OutputStreamWriter, BufferedWriter, File, FileOutputStream}
import java.util.zip.GZIPOutputStream

import Main.{WriterClosed, InsertJoinedSubject, Finalize}
import akka.actor.Actor

/**
 * Created by Chile on 8/23/2015.
 */
class JoinedKbCollector(filename: String) extends Actor {
  val zip = new GZIPOutputStream(new FileOutputStream(new File(filename)))
  val writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"))
  var counter = 0

  override def receive: Receive =
  {
    case InsertJoinedSubject(model) =>
    {
      counter += model.lines.size
      writer.append(model)
    }
    case Finalize() =>
    {
      System.out.println("output file " + filename + " has " + counter + " lines")
      writer.close()
      context.parent ! WriterClosed(filename)
    }
    case _ =>
  }
}
