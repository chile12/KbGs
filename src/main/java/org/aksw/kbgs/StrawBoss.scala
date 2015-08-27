package org.aksw.kbgs

import akka.actor.{Actor, ActorRef, Props}
import org.aksw.kbgs.Main._

import scala.collection.mutable._
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/27/2015.
 */
class StrawBoss extends Actor{

  private val workMap = new HashMap[String, ((() => _ <: Any), Queue[_ <: Any])]()
  private val workerMap = new HashMap[String, MutableList[ActorRef]]()
  private val queueSize = 30

  def registerNewProcess[T <: Any, W <: Any](intStruct: InitProcessStruct[T, W]) (implicit tag: ClassTag[T]): Unit =
  {
    workMap.put(intStruct.broadcastId, (intStruct.nextPackage, new Queue[W]()))
    val wl = workerMap.put(intStruct.broadcastId, new MutableList[ActorRef]()).get
    for(i <- 0 until intStruct.workerCount)
    {
      wl += context.actorOf(Props(tag.runtimeClass, intStruct.actorSigObjcts))
    }
  }

  def initializeWorkers(id: String, inits: scala.Seq[Any]): Unit =
  {
    for(worker <- workerMap.get(id).map(x => x.head))
      worker ! InitializeWorker(id, inits)
  }

  private def loadMoreWork (queue: Queue[_], workLoadFunction: () => _): Unit =
  {
    while(queue.size < queueSize)
    {
      queue += workLoadFunction()
    }
  }

  override def receive: Receive =
  {
    case RegisterNewProcess(intStruct) =>
    {
      registerNewProcess(intStruct)
    }
    case GimmeWork(id : String) => {
      val zw = workMap.get(id).get
      val zww = zw._2.headOption
      if (zww != None)
        sender ! Work(zww.get)
      loadMoreWork(workMap.get(id))
    }
    case InitializeWorker(id, inits) => {
      initializeWorkers(id, inits)
      sender ! WorkersInitialized(id)
    }
    case _ =>
  }

}

class InitProcessStruct[T, W <: AnyVal]{
  var broadcastId: String = null
  var workerCount: Integer = null
  var nextPackage:() => W = null
  var actorSigObjcts: Array[Any] = null
}