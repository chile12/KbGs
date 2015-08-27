package org.aksw.kbgs.processors

import akka.actor.{Actor, ActorRef}
import org.aksw.kbgs.{InitProcessStruct, Main}
import org.aksw.kbgs.Main._
import org.aksw.kbgs.inout.InstanceReader


/**
 * Created by Chile on 8/26/2015.
 */
class PropertyCompProcessor(boss: ActorRef, evalWriter: ActorRef)  extends Actor with InstanceProcessor[Unit]{
  private var inputEmpty = false
  override def startProcess(): Unit =
  {
    val sortedFileName = Main.config.outFile.substring(0, Main.config.outFile.indexOf(".nq.gz")) + "Sorted.nq.gz"
    //Main.mergeSort(Main.config.outFile, sortedFileName )

    val instanceReader = new InstanceReader[Unit](sortedFileName)
    val inits = new InitProcessStruct[KbComparatWorker, StringBuilder]()
    inits.broadcastId = "compareKb"
    inits.nextPackage = instanceReader.readNextSubject
    inits.workerCount = 4
    boss ! RegisterNewProcess(inits)
    evalWriter ! WriterStart("propProc", self.path.name)
    val zw = new Array[ActorRef](1)
    zw.update(0, evalWriter)
    boss ! InitializeWorker("compareKb", zw)
//    while(instanceReader.notFinished())
//      instanceReader.readSubject(evaluate, action)
//    inputEmpty = true
  }

  override def evaluate(input: StringBuilder): Unit =
  {
    if(input.getClass() == classOf[StringBuilder]) {
      val isb: StringBuilder = input.asInstanceOf[StringBuilder]
      evalWorkers ! DoComparisonFor(evalWriter, isb.toString())
    }
    else
      Main.logger.warning("Actor " + self.path.name + ": evaluate function was not called with a StringBuilder")

  }

  override def action(evalResult: Unit): Unit =
  {
    if(inputEmpty) {
      evalWriter ! Finalize
    }
  }

  override def finish(): Unit =
  {

  }

  override def receive: Receive =
  {
    case StartProcess =>
      startProcess
    case _ =>
  }

}
