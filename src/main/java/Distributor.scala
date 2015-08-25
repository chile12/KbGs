import Main.{Finalize, Finished, StartReader, WriterClosed}
import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParHashMap

/**
 * Created by Chile on 8/23/2015.
 */
class Distributor() extends Actor{

  private val outputWriter = context.actorOf(Props(classOf[JoinedKbCollector],Main.config.tempFile))
  private val kbMap : ParHashMap[String, (ActorRef, String)] = new mutable.HashMap[String, (ActorRef, String)]().par
  for(kbSpecs <- Main.config.kbMap)
  {
    val actor = context.actorOf(Props(classOf[KbReader], outputWriter, kbSpecs._1))
    kbMap.put(kbSpecs._1, (actor, ""))
  }


  override def receive: Receive =
  {
    case Finished(kbid) =>
    {
      kbMap.update(kbid, (sender, null))
      for(zz <- kbMap.values)
        if(zz._2 != null)
          return ConfigImpl.DefaultReceive
      outputWriter ! Finalize()
    }
    case StartReader =>
    {
      kbMap.values.map(x => x._1 ! StartReader())
    }
    case WriterClosed(tempFileName) =>
    {
      //TODO merge sort is buggy
      //new ParallelMergeSort().sort(Main.getInputStream(tempFileName), Main.config.outFile)
      //TODO add code for multiple writers!
      context.system.shutdown()

    }
    case _ =>
  }
}
