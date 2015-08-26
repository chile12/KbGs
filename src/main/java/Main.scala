import java.io.{InputStream, FileInputStream, BufferedInputStream}
import java.util.logging.{Level, Logger}
import java.util.zip.GZIPInputStream

import akka.actor.{ActorRef, ActorSystem, Props}

import scala.io.BufferedSource

/**
 * Created by Chile on 8/23/2015.
 */
object Main {
  val logger = Logger.getLogger("");
  logger.setLevel(Level.ALL)
  var config: ConfigImpl = null

  def main(args: Array[String]) {
    assert((args.length == 1))
    val actorSystem = ActorSystem()
    config = new ConfigImpl(args(0))
    val distributor = actorSystem.actorOf(Props(classOf[Distributor]))
    distributor ! StartProcess
    //distributor ! StartSecondPass
  }

  def getSource(path: String) : BufferedSource =
  {
    if(path.trim().endsWith(".gz"))
      new BufferedSource(getInputStream(path))
    else
      new BufferedSource(getInputStream(path))
  }

  def getInputStream(path: String) : InputStream =
  {
    if(path.trim().endsWith(".gz"))
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(path)))
    else
      new BufferedInputStream(new FileInputStream(path))
  }

  case class Finalize()
  case class InsertJoinedSubject(subj: StringBuilder)
  case class StartProcess()
  case class Finished(idBuffer: ConcurrentIdBuffer)
  case class WriterClosed(actor: String, fileName: String)
  case class WriterStart(fileName: String, actor: String)
  case class SameAsFinished()
  case class FinishProcessor()
  case class NewWriter()
  case class NewWriterResponse(writer: ActorRef)
  case class StartSameAsActor(filenames: Array[String], idBuffer: ConcurrentIdBuffer)
}
