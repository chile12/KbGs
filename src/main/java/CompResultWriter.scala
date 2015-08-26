
import java.io._

import Main._
import akka.actor.Actor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.HashBasedTable

import scala.collection.mutable

/**
 * Created by Chile on 8/26/2015.
 */
class CompResultWriter extends Actor{


  val propResultMap = HashBasedTable.create[String, String, mutable.HashMap[String, (Int, Int)]]()

  override def receive: Receive =
  {
    case AddCompResult(kb1: String, kb2: String, property: String, result: (Int, Int)) =>
    {
      var kbc = propResultMap.get(kb1, kb2)
      if(kbc == null)
        kbc = propResultMap.put(kb1, kb2, new mutable.HashMap[String, (Int, Int)])


      kbc.update(property, (kbc.get(property).get._1 + result._1, kbc.get(property).get._2 + result._2))
    }
    case Finalize() =>
    {
      val zip = new FileOutputStream(new File(Main.config.propEvalFile))
      val writer = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"))
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.writeValue(writer, propResultMap)
      writer.flush()
      writer.close()
      context.parent ! WriterClosed("", Main.config.propEvalFile)
    }
    case _ =>
  }
}
