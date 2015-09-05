package org.aksw.kbgs.processors

import akka.actor.{Actor, PoisonPill, Props}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.InstanceReader
import org.aksw.kbgs.workers.KbPathFilter
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 9/3/2015.
 */
class PropertyFilterProcessor(kbPrefix: String, stage: Int, idBuffer : HashMultimap[String, (String, String)], propertyMap: collection.mutable.HashMap[String, String]) extends Actor with InstanceProcessor[StringBuilder, String]{

  private var instanceReader: InstanceReader = null
  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))

  override def startProcess(): Unit =
  {
    instanceReader = new InstanceReader(List(Main.config.kbMap.get(kbPrefix).get("kbInput")))
    val inits = new InitProcessStruct()
    inits.broadcastId = "filterKb"
    inits.workerCount = Main.config.numberOfThreads
    val zz = classOf[KbPathFilter]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](kbPrefix, idBuffer, propertyMap)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(null)
    System.out.println("initialize KbPathFilter workers")
  }

  override def evaluate(input: StringBuilder): Future[String] = Future{
    null
  }

  override def action(evalResult: String): Unit =
  {

  }

  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case Finished =>
      finish()
    case _ =>
  }

  override def finish(): Unit =
  {
    context.parent ! ProcessorFinished(kbPrefix, stage)
    self ! PoisonPill
  }
}
