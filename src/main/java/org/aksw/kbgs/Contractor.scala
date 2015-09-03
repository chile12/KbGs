package org.aksw.kbgs

import java.util

import akka.actor.{PoisonPill, Actor, ActorRef, Props}
import akka.routing.{Broadcast, BroadcastRouter}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.helpers.IdBuffer
import org.aksw.kbgs.inout.WorkLoader

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/27/2015.
 */
class Contractor[W] extends Actor{

  private var workers: ActorRef = null
  private var wLoader: WorkLoader[W] = null
  private var workerCount = 0
  private var client: ActorRef = null
  private val broadCaster = new util.ArrayList[ActorRef]()
  private val finishedList = new util.ArrayList[Future[Any]]()
  private var finalizedWorkers =0

  def registerNewProcess(intStruct: InitProcessStruct, wLoader: WorkLoader[W]): Unit =
  {
    broadCaster.clear()
    finishedList.clear()
    this.wLoader = wLoader
    this.workerCount = intStruct.workerCount
    workers =
      if(intStruct.actorSigObjcts != null)
        context.actorOf(Props(Props.defaultDeploy, intStruct.classTag.runtimeClass, intStruct.actorSigObjcts).withRouter(BroadcastRouter(intStruct.workerCount)))
      else
        context.actorOf(Props(intStruct.classTag.runtimeClass).withRouter(BroadcastRouter(intStruct.workerCount)))
    workers ! AssignWorkers(self)
    client ! ContractSigned
  }

  override def receive: Receive =
  {
    case RegisterNewWorkPackage(intStruct: InitProcessStruct, wLoader: WorkLoader[W]) =>
    {
      client = sender
      registerNewProcess(intStruct, wLoader)
    }
    case GimmeWork() => {
      val zw = wLoader.next()
      if( zw != null && zw != None)
        sender ! Work(zw)
      else {
        sender ! Finalize
      }
    }
    case InitializeWorker(inits) => {
      if(wLoader != null)
      {
        System.out.println("initialize workers")
        workers ! Broadcast(InitializeWorker(inits))
        sender ! WorkersInitialized()
      }
      else
        sender ! Finished(Option(throw new UninitializedError()))
    }
    case RegisterNewBroadcaster(actor) => {
      broadCaster.add((actor))
    }
    case UnregisterBroadcaster(actor) =>
      broadCaster.remove(actor)
    case Broadcast(any) =>
      if(broadCaster.contains(sender))
        workers ! Broadcast(any)
    case Finished =>
    {
      finalizedWorkers = finalizedWorkers+1
      if(workerCount == finalizedWorkers)
      {
        client ! Finished
        context.actorSelection("/user/distributor") ! Finished
        self ! PoisonPill
      }
    }
    case _ =>
  }
}

object Contractor{
  case class RegisterNewBroadcaster(bCaster: ActorRef)
  case class UnregisterBroadcaster(bCaster: ActorRef)
  case class Finished(result: Option[Any])
  case class IdBufferMsg(bufferAcor: ActorRef, buffer: mutable.HashMap[String, mutable.Set[String]])
  case class Finalize()
  case class InsertJoinedSubject(subj: StringBuilder)
  case class StartProcess()
  case class WriterClosed(actor: String, fileName: String)
  case class WriterStart(fileName: String, actor: String, gzip: Boolean = true)
  case class SameAsFinished()
  case class FinishProcessor()
  case class NewWriter()
  case class NewWriterResponse(writer: ActorRef)
  case class StartSameAsActor(filenames: List[String])
  case class AddCompResult(kb1: String, kb2: String, property: String, result: (Option[Float], Int))
  case class DoComparisonFor(writer: ActorRef, inpiut: String)
  case class GimmeWork()
  case class InitializeWorker(inits: Seq[Any])
  case class Work[T](work: T)
  case class RegistrateNewWriterSource()
  case class NoMoreWork()
  case class RegisterNewWorkPackage[W, T](inits: InitProcessStruct, wl: WorkLoader[W])
  case class RemoveId(id: String)
  case class ContractSigned()
  case class AssignWorkers(boss: ActorRef)
  case class WorkersInitialized()
}

class InitProcessStruct{
  var broadcastId: String = null
  var workerCount: Integer = null
  var actorSigObjcts: scala.collection.immutable.Seq[scala.Any] = null
  var classTag : ClassTag[_] = null
  var workLoader: String = null
  var idBuffer: IdBuffer = null
}