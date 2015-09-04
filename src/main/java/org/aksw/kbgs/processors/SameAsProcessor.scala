package org.aksw.kbgs.processors

import akka.actor.{Actor, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.{InstanceReader, WriterActor}
import org.aksw.kbgs.workers.SameAsWorker
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}
import org.apache.commons.lang3.SystemUtils

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/25/2015.
 */
class SameAsProcessor(tempUriMap: mutable.HashMap[String, String]) extends Actor with InstanceProcessor[StringBuilder, Unit] {

  private var filenames: List[String] = null
  private var instanceReader: InstanceReader = null
  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val outputWriter = context.actorOf(Props(classOf[WriterActor]), "outputWriter")

  override def startProcess(): Unit =
  {
    outputWriter ! WriterStart(Main.config.unsorted, outputWriter.path.name, false)
    instanceReader = new InstanceReader(filenames)
    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = 4
    val zz = classOf[SameAsWorker]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](outputWriter)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(Seq(tempUriMap))
  }

  override def evaluate(input: StringBuilder): Future[Unit] = Future{

  }

  override def action(evalResult: Unit): Unit =
  {

  }

  override def receive: Receive =
  {
    case StartSameAsActor(fNames) => {
      filenames = fNames
      startProcess()
    }
    case Finished =>
      finish()
    case _ =>
  }

  override def finish(): Unit =
  {
    outputWriter ! Finalize
    if(SystemUtils.IS_OS_WINDOWS)
      true //TODO delete on windows
    else if(SystemUtils.IS_OS_UNIX)
      for(filename <- filenames)
        scala.sys.process.Process("rm -rf " + filename).!
    context.parent ! SameAsFinished()
    self ! PoisonPill
  }
}
