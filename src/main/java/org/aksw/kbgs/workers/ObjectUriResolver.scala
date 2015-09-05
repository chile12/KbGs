package org.aksw.kbgs.workers

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Contractor._
import org.aksw.kbgs.helpers.NQuadStatement
import org.apache.commons.io.input.CharSequenceReader
import org.openrdf.model.{Statement, URI}
import org.openrdf.rio.RDFHandler
import org.openrdf.rio.nquads.NQuadsParser

import scala.collection.mutable

/**
 * Created by Chile on 9/4/2015.
 */
class ObjectUriResolver(writer: ActorRef, valueMap: mutable.HashMap[String, String]) extends Actor with RDFHandler{
  private val objectIsUri = HashMultimap.create[String, String]()
  private var boss: ActorRef = null
  private val parser = new NQuadsParser()
  parser.setRDFHandler(this)
  private var sb: StringBuilder = null

  def doWork(input: StringBuilder): Unit =
  {
    parser.parse(new CharSequenceReader(input), "")
  }

  override def startRDF(): Unit =
  {
    sb = new StringBuilder()
  }

  override def handleStatement(statement: Statement): Unit =
  {
    if(statement.getObject.isInstanceOf[URI]) //not!,  object is uri
    {
      valueMap.get("<" + statement.getObject.stringValue() + ">") match{
        case Some(v) =>
        {
          val newSt = new NQuadStatement(statement.getSubject.stringValue(), statement.getPredicate.stringValue(), v, statement.getContext.stringValue())
          sb.append(NQuadStatement.statementToNQuadString(newSt))
          return
        }
        case None =>
      }
    }
    val test = NQuadStatement.statementToNQuadString(statement)
    sb.append(NQuadStatement.statementToNQuadString(statement))
  }

  override def handleComment(s: String): Unit = {}

  override def handleNamespace(space: String, prefix: String): Unit = {}

  override def endRDF(): Unit =
  {
    writer ! InsertJoinedSubject(sb)
    boss ! GimmeWork()
  }

  override def receive: Receive = {
    case InitializeWorker(inits) =>
    {
      writer ! RegistrateNewWriterSource
      boss ! GimmeWork()
    }
    case Work(work) =>
    {
      val zw = work.asInstanceOf[Option[StringBuilder]]
      zw.map(doWork(_))
    }
    case AssignWorkers(b) =>
    {
      boss = b
    }
    case Finalize => {
      boss ! Finished(None)//Option(("", (None, None, Option(objectIsUri)))))
      self ! PoisonPill
    }
    case _ =>
  }
}
