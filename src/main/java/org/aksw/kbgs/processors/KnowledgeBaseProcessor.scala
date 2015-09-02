package org.aksw.kbgs.processors

import akka.actor.{Props, Actor, ActorRef}
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.helpers.IdBuffer
import org.aksw.kbgs.inout.InstanceReader
import org.aksw.kbgs.workers.KbFilter
import org.aksw.kbgs.{Contractor, InitProcessStruct, Main}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by Chile on 8/23/2015.
 */
class KnowledgeBaseProcessor(tempWriter: ActorRef, kbPrefix: String) extends Actor with InstanceProcessor[StringBuilder, String]{

  private var finished = false
  private val contractor = context.actorOf(Props(classOf[Contractor[StringBuilder]]))
  private val idBuffer = new IdBuffer(Main.config.kbMap.get(kbPrefix).get("idFile"), -1)

  //first pass: evaluate all instances from sorted source file
  override def startProcess(): Unit =
  {
    tempWriter ! WriterStart(getTempWriterName, self.path.name)
    val instanceReader = new InstanceReader(List(Main.config.kbMap.get(kbPrefix).get("kbInput")))
    val inits = new InitProcessStruct()
    inits.broadcastId = "compareKb"
    inits.workerCount = 4
    val zz = classOf[KbFilter]
    inits.classTag = ClassTag(zz)
    inits.actorSigObjcts = scala.collection.immutable.Seq[scala.Any](tempWriter, kbPrefix,idBuffer.getMap())
    contractor ! RegisterNewWorkPackage(inits, instanceReader)
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
    tempWriter ! Finalize
    finished = true
  }

  private def getTempWriterName(): String =
  {
    Main.config.tempFile.substring(0,
      Main.config.tempFile.lastIndexOf('.')) + "_" + kbPrefix +
      Main.config.tempFile.substring(Main.config.tempFile.lastIndexOf('.'))
  }

  override def receive: Receive =
  {
    case StartProcess() =>
      startProcess()
    case Finished =>
    {
      finish()
      context.stop(self)
    }
    case ContractSigned =>
    {
      System.out.println("contract signed")
      contractor ! InitializeWorker(null)
    }
    case _ =>
  }

}
