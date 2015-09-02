package org.aksw.kbgs.processors

import java.io.File

import akka.actor.{Actor, ActorRef}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.InstanceReader
import org.aksw.kbgs.workers.KbComparatWorker
import org.aksw.kbgs.{InitProcessStruct, Main}
import org.apache.commons.lang3.SystemUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
/**
 * Created by Chile on 8/26/2015.
 */
class PropertyCompProcessor(contractor: ActorRef, evalWriter: ActorRef)  extends Actor with InstanceProcessor[StringBuilder, Unit]{
  private var inputEmpty = false
  override def startProcess(): Unit =
  {
    val sortedFileName =  new File(Main.config.outFile.substring(0, Main.config.outFile.indexOf(".nq.gz")) + "Sorted.nq.gz")
    System.out.println("before sorting")
    if(SystemUtils.IS_OS_WINDOWS)
       true //TODO merge sort on windows
    else if(SystemUtils.IS_OS_UNIX)
      scala.sys.process.Process("sort --parallel=8 -uo " + sortedFileName.getAbsolutePath + " " + Main.config.outFile).!
    System.out.println("sorting done :)")

    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = 4
    val zz = classOf[KbComparatWorker]
    inits.classTag = ClassTag(zz)
    contractor ! RegisterNewWorkPackage(inits, new InstanceReader(List(sortedFileName.getAbsolutePath)))
    evalWriter ! WriterStart("propProc", self.path.name)

    val zw = new Array[ActorRef](1)
    zw.update(0, evalWriter)
    contractor ! InitializeWorker(zw)
  }

  override def evaluate(input: StringBuilder): Future[Unit] = Future
  {
  }

  override def action(evalResult: Unit): Unit =
  {
  }

  override def finish(): Unit =
  {
    evalWriter ! Finalize
    context.stop(self)
  }

  override def receive: Receive =
  {
    case StartProcess =>
      startProcess
    case Finished =>
      finish()
    case _ =>
  }

}
