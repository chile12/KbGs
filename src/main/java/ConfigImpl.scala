import java.io.File

import akka.actor.Actor.Receive

import scala.io.BufferedSource
import scala.util.parsing.json.JSON

/**
 * Created by Chile on 3/18/2015.
 */
class ConfigImpl(path: String) {

  private val file = new File(path)
  private var source: BufferedSource = null
  private val jsonString: String = if(file.exists()) {source = scala.io.Source.fromFile(file); source.mkString} else path
  source.close()

  private val config : Map[String, Any] = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]

  val kbMap = config.get("kbMap").get.asInstanceOf[Map[String, Map[String, String]]]
  val outFile = config.get("outFile").get.toString

}

object ConfigImpl{
  val DefaultReceive = new Receive {
    def apply(any: Any) = {}
    def isDefinedAt(any: Any) = false
  }
}
