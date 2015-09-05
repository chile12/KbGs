package org.aksw.kbgs.processors

import akka.actor.{Actor, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.{InstanceReader, WriterActor}
import org.aksw.kbgs.workers.ObjectUriResolver
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 9/4/2015.
 */
class ObjectUriProcessor(fileNames: List[String], valueMap: collection.mutable.HashMap[String, String])  extends Actor with InstanceProcessor[StringBuilder, String]{

  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val tempWriter = context.actorOf(Props(classOf[WriterActor]), "temp2")

  override def startProcess(): Unit =
  {
    tempWriter ! WriterStart(Main.config.tempFile)
    val instanceReader = new InstanceReader(fileNames)
    val inits = new InitProcessStruct()
    inits.broadcastId = "resolve"
    inits.workerCount = Main.config.numberOfThreads
    val zz = classOf[ObjectUriResolver]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](tempWriter, valueMap)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(null)
    System.out.println("initialize ObjectUriResolver workers")
  }

  override def action(evalResult: String): Unit = {}

  override def finish(): Unit = {}

  override def evaluate(input: StringBuilder): Future[String] = {Future{null}}


  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case Finished =>
    {
      tempWriter ! Finalize
    }
    case WriterClosed(filename) =>
    {
      context.parent ! UriPathsResolved(Main.config.tempFile)
      self ! PoisonPill
    }
    case _ =>
  }


}
