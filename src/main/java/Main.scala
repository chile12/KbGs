import java.io.{InputStream, FileInputStream, BufferedInputStream}
import java.util.logging.{Level, Logger}
import java.util.zip.GZIPInputStream

import akka.actor.{ActorSystem, Props}

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
    distributor ! StartReader
  }

  def getSource(path: String) : Iterator[String] =
  {
    if(path.trim().endsWith(".gz"))
      new BufferedSource(getInputStream(path)).getLines()
    else
      new BufferedSource(getInputStream(path)).getLines()
  }

  def getInputStream(path: String) : InputStream =
  {
    if(path.trim().endsWith(".gz"))
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(path)))
    else
      new BufferedInputStream(new FileInputStream(path))
  }

  case class EvalRequest(input: StringBuilder, idBuffer: ConcurrentIdBuffer)
  case class EvalResponse(uri: String)
  case class Finalize()
  case class InsertJoinedSubject(subj: StringBuilder)
  case class StartReader()
  case class Finished(kbid: String)
  case class WriterClosed(fileName: String)
}
