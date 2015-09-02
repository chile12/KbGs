package org.aksw.kbgs.processors

import akka.actor.{Actor, ActorRef}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.InstanceReader
import org.aksw.kbgs.workers.SameAsWorker
import org.aksw.kbgs.{InitProcessStruct, Main}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/25/2015.
 */
class SameAsProcessor(contractor: ActorRef, idBuffer: HashMultimap[String, String], outputWriter: ActorRef) extends Actor with InstanceProcessor[StringBuilder, Unit] {

  var filenames: List[String] = null
  //second pass: resolve same as links -> one resource has just one identifier
  override def startProcess(): Unit =
  {
    outputWriter ! WriterStart(Main.config.outFile, outputWriter.path.name)
    val instanceReader = new InstanceReader(filenames)
//      while (instanceReader.notFinished())
//        evaluate(instanceReader.readNextSubject()).onSuccess{case s: Unit => action(s)}
    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = 4
    val zz = classOf[SameAsWorker]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](idBuffer, outputWriter)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(null)
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
    context.parent ! SameAsFinished()
    context.stop(self)
  }
}
