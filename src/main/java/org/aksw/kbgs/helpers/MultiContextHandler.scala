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
    val m = modelMap.get(context.stringValue())
    if(m != null)
      m.get.add(st)
    else
      System.out.print(st.toString)
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