package org.aksw.kbgs.processors

import akka.actor.{Actor, PoisonPill, Props}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.helpers.IdBuffer
import org.aksw.kbgs.inout.{InstanceReader, WriterActor}
import org.aksw.kbgs.workers.KbFilter
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/23/2015.
 */
class KnowledgeBaseProcessor(kbPrefix: String, propertyMap: collection.mutable.HashMap[String, String], isUriProvider: Boolean = false) extends Actor with InstanceProcessor[StringBuilder, String]{

  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val idBuffer = new IdBuffer(Main.config.kbMap.get(kbPrefix).get("idFile"), -1)
  private val tempWriter = context.actorOf(Props(classOf[WriterActor]), kbPrefix + "temp")

  //first pass: evaluate all instances from sorted source file
  override def startProcess(): Unit =
  {
    tempWriter ! WriterStart(Main.config.getTempFile(kbPrefix, 1, true))
    val instanceReader = new InstanceReader(List(Main.config.kbMap.get(kbPrefix).get("kbInput")))
    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = Main.config.numberOfThreads
    val zz = classOf[KbFilter]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](tempWriter, kbPrefix, idBuffer.getMap(), propertyMap, isUriProvider)
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
    contractor ! InitializeWorker(null)
    System.out.println("initialize KbFilter workers")

  }

  override def evaluate(input: StringBuilder): Future[String] =
  Future{
      ""
    }


  override def action(uri: String): Unit =
  {
  }


  override def finish(): Unit =
  {
  }

  private def getProperyList()=
  {
    val propList = new ListBuffer[String]()
    for(p <- Main.config.properties)
      p._2.get(kbPrefix).map(x => propList += x)
  }

  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case Finished =>
    {
      tempWriter ! Finalize
    }
    case WriterClosed(filename) =>
    {
      context.parent ! ProcessorFinished(kbPrefix, 0)
      self ! PoisonPill
    }
    case ContractSigned =>
    {
      System.out.println("contract signed")
    }
    case _ =>
  }

}
