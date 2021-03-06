package org.aksw.kbgs.processors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.inout.{CompResultWriter, InstanceReader}
import org.aksw.kbgs.workers.KbComparatWorker
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}
import org.apache.commons.lang3.SystemUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag
/**
 * Created by Chile on 8/26/2015.
 */
class PropertyCompProcessor()  extends Actor with InstanceProcessor[StringBuilder, Unit]{
  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val evalWriter = context.actorOf(Props(classOf[CompResultWriter]), "evalWriter")
  override def startProcess(): Unit =
  {
    System.out.println("before sorting")
    if(SystemUtils.IS_OS_WINDOWS)
       true //TODO merge sort on windows
    else if(SystemUtils.IS_OS_UNIX)
    {
      scala.sys.process.Process("sort --parallel=8 -uo " + Main.config.sorted + " " + Main.config.unsorted).!
      scala.sys.process.Process("gzip -f " + Main.config.sorted).!
      scala.sys.process.Process("rm -rf " + Main.config.unsorted).!
    }
    System.out.println("sorting done :)")

    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = Main.config.numberOfThreads
    val zz = classOf[KbComparatWorker]
    inits.classTag = ClassTag(zz)
    contractor ! RegisterNewWorkPackage(inits, new InstanceReader(List(Main.config.sorted + ".gz")))

    val zw = new Array[ActorRef](1)
    zw.update(0, evalWriter)
    contractor ! InitializeWorker(zw)
    System.out.println("initialize KbComparatWorker workers")
  }

  override def evaluate(input: StringBuilder): Future[Unit] = Future
  {
  }

  override def action(evalResult: Unit): Unit =
  {
  }

  override def finish(): Unit =
  {
  }

  override def receive: Receive =
  {
    case StartProcess =>
      startProcess
    case Finished =>
      evalWriter ! Finalize
    case WriterClosed(filename) =>
    {
      if(SystemUtils.IS_OS_WINDOWS)
        true //TODO delete on windows
      else if(SystemUtils.IS_OS_UNIX)
        scala.sys.process.Process("rm -rf " + Main.config.sorted + ".gz").!

      context.parent ! CompProcFinished()
      self ! PoisonPill
    }
    case _ =>
  }

}
