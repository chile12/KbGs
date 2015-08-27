package org.aksw.kbgs.processors

import org.aksw.kbgs.Main
import Main._
import akka.actor.{Actor, ActorRef}
import org.aksw.kbgs.helpers.ConcurrentIdBuffer
import org.aksw.kbgs.inout.InstanceReader


/**
 * Created by Chile on 8/25/2015.
 */
class SameAsProcessor(outputWriter: ActorRef) extends Actor with InstanceProcessor[Unit] {

  var filenames: Array[String] = null
  var idBuffer:ConcurrentIdBuffer = null
  //second pass: resolve same as links -> one resource has just one identifier
  override def startProcess(): Unit =
  {
    outputWriter ! WriterStart(Main.config.outFile, outputWriter.path.name)
    idBuffer.normalizeSameAsLinks()
    for(filename <- filenames) {
      val instanceReader = new InstanceReader[Unit](filename)
      while (instanceReader.notFinished())
        instanceReader.readSubject(evaluate, action)
    }
    outputWriter ! Finalize()
    context.parent ! SameAsFinished()
  }

  override def evaluate(input: StringBuilder): Unit = {
      val isb: StringBuilder = input.asInstanceOf[StringBuilder]
      val sb = new StringBuilder()
      val firstLine = isb.lines.next()
      val subject = firstLine.substring(0, firstLine.indexOf(">") + 1)
      val sameAsValues = idBuffer.getSameAs(subject)
      if (sameAsValues.size > 0) {
        for (line <- isb.lines)
          sb.append(line.replace(subject, sameAsValues(0)))
        outputWriter ! InsertJoinedSubject(sb)
      }
      outputWriter ! InsertJoinedSubject(isb)
  }

  override def action(evalResult: Unit): Unit =
  {

  }

  override def receive: Receive =
  {
    case StartSameAsActor(fNames, buf) => {
      filenames = fNames
      idBuffer = buf
      startProcess()
    }
    case FinishProcessor() =>
      finish()
    case _ =>
  }

  override def finish(): Unit = ???
}
