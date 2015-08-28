package org.aksw.kbgs.processors

import akka.actor.{Actor, ActorRef}
import org.aksw.kbgs.Main._
import org.aksw.kbgs.{InitProcessStruct, Main}

import scala.reflect.ClassTag


/**
 * Created by Chile on 8/26/2015.
 */
class PropertyCompProcessor(boss: ActorRef, evalWriter: ActorRef)  extends Actor with InstanceProcessor[Unit]{
  private var inputEmpty = false
  override def startProcess(): Unit =
  {
    val sortedFileName = Main.config.outFile.substring(0, Main.config.outFile.indexOf(".nq.gz")) + "Sorted.nq.gz"
    sys.process.stringSeqToProcess(Seq("bash", "-c", "sort --parallel=8 -uo " + sortedFileName + " " + Main.config.outFile))
    val inits = new InitProcessStruct[StringBuilder]()
    inits.broadcastId = "compareKb"
    inits.sourceFile = sortedFileName
    inits.workerCount = 4
    val zz = classOf[KbComparatWorker]
    inits.classTag = ClassTag(zz)
    boss ! RegisterNewProcess(inits)
    evalWriter ! WriterStart("propProc", self.path.name)

    val zw = new Array[ActorRef](1)
    zw.update(0, evalWriter)
    boss ! InitializeWorker("compareKb", zw)
  }

  override def evaluate(input: StringBuilder): Unit =
  {
  }

  override def action(evalResult: Unit): Unit =
  {
  }

  override def finish(): Unit =
  {
    evalWriter ! Finalize
  }

  override def receive: Receive =
  {
    case StartProcess =>
      startProcess
    case NoMoreWork(id) =>
      finish()
    case _ =>
  }

}
