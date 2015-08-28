package org.aksw.kbgs.inout

import java.io._
import java.util

import akka.actor.{Actor, ActorRef}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.google.common.collect.HashBasedTable
import org.aksw.kbgs.Main
import org.aksw.kbgs.Main.{AddCompResult, Finalize, RegistrateNewWriterSource, WriterClosed}

import scala.collection.mutable

/**
 * Created by Chile on 8/26/2015.
 */
class CompResultWriter extends Actor{

  val propResultTable = new util.HashMap[String, Array[java.lang.Float]]()
  val propResultMap = HashBasedTable.create[String, String, mutable.HashMap[String, (Float, Int)]]()
  var sourceMap = new mutable.HashMap[ActorRef, Boolean]()

  override def receive: Receive =
  {
    case AddCompResult(kb1: String, kb2: String, property: String, result: (Float, Int)) =>
    {
      var value = propResultTable.get(kb1 + "," + kb2 + "," + property)
      if(value == null)
        value = propResultTable.get(kb2 + "," + kb1 + "," + property)
      if(value == null)
      {
        propResultTable.put(kb1 + "," + kb2 + "," + property, Array(0f, 0f))
        value = propResultTable.get(kb1 + "," + kb2 + "," + property)
      }

      value(0) = value(0) + result._1
      value(1) = value(1) + result._2
    }
    case Finalize =>
    {
      if(sourceMap.keySet.contains(sender))
        sourceMap.update(sender, true)
      if(sourceMap.values.forall((x) => x == true)) {
        val mapper = new ObjectMapper()
        mapper.registerModule(new GuavaModule())
        mapper.writeValue(new File(Main.config.propEvalFile), propResultTable)

        context.parent ! WriterClosed("", Main.config.propEvalFile)
      }
    }
    case RegistrateNewWriterSource =>
      if(!sourceMap.keySet.contains(sender))
        sourceMap.put(sender, false)
    case _ =>
  }
}
