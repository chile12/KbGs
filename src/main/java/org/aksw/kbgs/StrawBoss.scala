package org.aksw.kbgs

import akka.actor.{Actor, ActorRef, Props}
import org.aksw.kbgs.Main._
import org.aksw.kbgs.inout.InstanceReader

import scala.collection.mutable._
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/27/2015.
 */
class StrawBoss extends Actor{

  private val workMap = new HashMap[String, InstanceReader[Unit]]()
  private val workerMap = new HashMap[String, MutableList[ActorRef]]()
  private val parentMap = new HashMap[String, ActorRef]()
  private val queueSize = 30

  def registerNewProcess[T <: Any, W <: Any](intStruct: InitProcessStruct[W]) (implicit tag: ClassTag[W]): Unit =
  {
    workMap.put(intStruct.broadcastId, (new InstanceReader[Unit](intStruct.sourceFile)))
    workerMap.put(intStruct.broadcastId, new MutableList[ActorRef]())
    val wl = workerMap.get(intStruct.broadcastId).get
    for(i <- 0 until intStruct.workerCount)
    {
      if(intStruct.actorSigObjcts != null)
        wl += context.actorOf(Props(intStruct.classTag.runtimeClass, intStruct.actorSigObjcts))
      else
        wl += context.actorOf(Props(intStruct.classTag.runtimeClass))
    }
  }

  def initializeWorkers(id: String, inits: scala.Seq[Any]): Unit =
  {
    for(worker <- workerMap.get(id).get)
      worker ! InitializeWorker(id, inits)
  }

  override def receive: Receive =
  {
    case RegisterNewProcess(intStruct) =>
    {
      parentMap.put(intStruct.broadcastId, sender)
      registerNewProcess(intStruct)
    }
    case GimmeWork(id : String) => {
      val zw = workMap.get(id).get.readNextSubject()
      if( zw != null && zw != None)
        sender ! Work(zw)
      else {
        sender ! Finalize
      }
    }
    case InitializeWorker(id, inits) => {
      initializeWorkers(id, inits)
      sender ! WorkersInitialized(id)
    }
    case _ =>
  }
}

class InitProcessStruct[W <: Any]{
  var broadcastId: String = null
  var workerCount: Integer = null
  var nextPackage:() => W = null
  var actorSigObjcts: Array[Any] = null
  var classTag : ClassTag[_] = null
  var sourceFile: String = null
}