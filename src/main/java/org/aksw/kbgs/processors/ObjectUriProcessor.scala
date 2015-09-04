package org.aksw.kbgs.processors

import akka.actor.{Actor, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.{InstanceReader, WriterActor}
import org.aksw.kbgs.workers.ObjectUriResolver
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 9/4/2015.
 */
class ObjectUriProcessor(kbPrefix: String, filenames: List[String], valueMap: collection.mutable.HashMap[String, String])  extends Actor with InstanceProcessor[StringBuilder, String]{

  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val tempWriter = context.actorOf(Props(classOf[WriterActor]), kbPrefix + "temp2")

  override def startProcess(): Unit =
  {
    tempWriter ! WriterStart(getTempWriterName, kbPrefix)
    val instanceReader = new InstanceReader(List(Main.config.kbMap.get(kbPrefix).get("kbInput")))
    val inits = new InitProcessStruct()
    inits.broadcastId = "resolve"
    inits.workerCount = 4
    val zz = classOf[ObjectUriResolver]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](kbPrefix, tempWriter, valueMap)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(null)
  }

  private def getTempWriterName(): String =
  {
    Main.config.tempFile.substring(0,
      Main.config.tempFile.lastIndexOf('.')) + "_2" + kbPrefix +
      Main.config.tempFile.substring(Main.config.tempFile.lastIndexOf('.')) + ".gz"
  }

  override def action(evalResult: String): Unit = ???

  override def finish(): Unit = ???

  override def evaluate(input: StringBuilder): Future[String] = ???


  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case Finished =>
    {
      finish()
      tempWriter ! Finalize
      context.parent ! UriPathsResolved(kbPrefix, getTempWriterName)
      self ! PoisonPill
    }
    case _ =>
  }


}
