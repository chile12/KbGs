package org.aksw.kbgs.helpers


import org.openrdf.model.{Model, Statement}
import org.openrdf.rio.RDFHandler

import scala.collection.mutable.HashMap

/**
 * Created by Chile on 8/26/2015.
 */
class MultiContextHandler(modelMap: HashMap[String, Model]) extends RDFHandler{

  override def handleStatement(st: Statement): Unit = {
    val context = st.getContext
    modelMap.get(context.stringValue()).map(_.add(st))
  }

  override def startRDF(): Unit =
  {}

  override def handleComment(s: String): Unit =
  {}

  override def endRDF(): Unit =
  {}

  override def handleNamespace(space: String, prefix: String): Unit =
  {
    for(model <- modelMap.values)
      model.setNamespace(prefix, space)
  }
}
