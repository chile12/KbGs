package org.aksw.kbgs.inout

import java.io._

import akka.actor.{Actor, ActorRef}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.google.common.collect.HashBasedTable
import org.aksw.kbgs.Contractor.{AddCompResult, Finalize, RegistrateNewWriterSource, WriterClosed}
import org.aksw.kbgs.Main

import scala.collection.mutable

/**
 * Created by Chile on 8/26/2015.
 */
class CompResultWriter extends Actor{

  val propResultTable = new mutable.HashMap[String, Array[java.lang.Float]]()
  val propResultMap = HashBasedTable.create[String, String, mutable.HashMap[String, (Float, Int)]]()
  var sourceMap = new mutable.HashMap[ActorRef, Boolean]()

  override def receive: Receive =
  {
    case AddCompResult(kb1: String, kb2: String, property: String, result: (Option[Float], Int)) =>
    {
      var value = propResultTable.get(kb1 + "," + kb2 + "," + property)
      value match {
        case None => {
          value = propResultTable.get(kb2 + "," + kb1 + "," + property)
          value match {
            case None =>{
              propResultTable.put(kb1 + "," + kb2 + "," + property, Array(0f, 0f))
              value = propResultTable.get(kb1 + "," + kb2 + "," + property)
            }
          }
        }
      }

      val v = value.get

      v(0) = v(0) + result._1.getOrElse(0f)
      v(1) = v(1) + 1f
    }
    case Finalize =>
    {
        val mapper = new ObjectMapper()
        mapper.registerModule(new GuavaModule())
        mapper.writeValue(new File(Main.config.propEvalFile), propResultTable)
        context.parent ! WriterClosed("", Main.config.propEvalFile)
    }
    case RegistrateNewWriterSource =>
      if(!sourceMap.keySet.contains(sender))
        sourceMap.put(sender, false)
    case _ =>
  }
}
