package org.aksw.kbgs.processors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.InstanceReader
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
class SameAsProcessor(temUriMap: mutable.HashMap[String, String], outputWriter: ActorRef) extends Actor with InstanceProcessor[StringBuilder, Unit] {

  var filenames: List[String] = null
  var instanceReader: InstanceReader = null
  val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  //second pass: resolve same as links -> one resource has just one identifier
  override def startProcess(): Unit =
  {
    outputWriter ! WriterStart(Main.config.unsorted, outputWriter.path.name, false)
    instanceReader = new InstanceReader(filenames)
//      while (instanceReader.notFinished())
//        evaluate(instanceReader.readNextSubject()).onSuccess{case s: Unit => action(s)}
    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = 4
    val zz = classOf[SameAsWorker]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](outputWriter)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(Seq(temUriMap))
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
