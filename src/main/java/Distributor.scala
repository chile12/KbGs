import java.util

import Main._
import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val outputWriter = context.actorOf(Props(classOf[JoinedKbCollector]), "output")
  private val kbMap : ParHashMap[String, (ActorRef, ActorRef)] = new mutable.HashMap[String, (ActorRef, ActorRef)]().par
  for(kbSpecs <- Main.config.kbMap)
  {
    val tempWriter = context.actorOf(Props(classOf[JoinedKbCollector]), kbSpecs._1 + "temp")
    val actor = context.actorOf(Props(classOf[KnowledgeBaseProcessor], tempWriter, kbSpecs._1), kbSpecs._1)
    kbMap.put(kbSpecs._1, (actor, tempWriter))
  }
  private var centralSameAsBuffer: ConcurrentIdBuffer = null
  private val tempfiles: util.ArrayList[String] = new util.ArrayList[String]()
  private val sameAsActor = context.actorOf(Props(classOf[SameAsProcessor],outputWriter), "sameas")


  override def receive: Receive =
  {
    case Finished(idBuffer) =>
    {
      if(centralSameAsBuffer == null)
        centralSameAsBuffer = idBuffer
      else
        centralSameAsBuffer.addSameAsBuffer(idBuffer)
    }
    case StartProcess =>
    {
      kbMap.values.map(x => x._1 ! StartProcess())
    }
    case StartSameAsActor(fnames, buf) =>
    {
      val arr = tempfiles.toArray(new Array[String](tempfiles.size))
      if(sameAsActor != null)
        sameAsActor ! StartSameAsActor(arr, centralSameAsBuffer)
      else
        Main.logger.severe("sameAs actor was not initialized yet!")
    }
    case WriterClosed(actor, filename) =>
    {
      //TODO merge sort is buggy
      //new ParallelMergeSort().sort(Main.getInputStream(tempFileName), Main.config.outFile)
      if(Main.config.outFile == filename)
        context.system.shutdown()
      if(this.kbMap.keySet.contains(actor))
      {
        kbMap.update(actor, (sender, null))
        tempfiles.add(filename)
        for(zz <- kbMap.values) {
          if (zz._2 != null)
            return ConfigImpl.DefaultReceive
        }
        self ! StartSameAsActor(null, null)
      }
    }
    case _ =>
  }
}
