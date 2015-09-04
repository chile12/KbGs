package org.aksw.kbgs.helpers

import org.openrdf.model.{Statement, Model}
import org.openrdf.rio.RDFHandler

/**
 * Created by Chile on 9/4/2015.
 */
class SimpleRdfHandler(model: Model) extends RDFHandler{
  override def endRDF(): Unit = ???

  override def handleComment(s: String): Unit = ???

  override def handleStatement(statement: Statement): Unit = model.add(statement)

  override def handleNamespace(space: String, prefix: String): Unit = model.setNamespace(prefix, space)

  override def startRDF(): Unit = ???
}
