package org.aksw.kbgs.helpers

import org.openrdf.model.impl.{LiteralImpl, URIImpl}
import org.openrdf.model._
/**
 * Created by Chile on 9/4/2015.
 */
class NQuadStatement() extends Statement{
  private var subject: URI = null
  private var predicate: URI = null
  private var objecct: Value = null
  private var context: URI = null
  
  def this(subj: URI, pred: URI, obj: Value, graph: URI)
  {
    this()
    subject = subj
    predicate = pred
    objecct = obj
    context = graph
  }

  def this(subjStr: String, predStr: String, objStr: String, graphStr: String)
  {
    this()
    subject = new URIImpl(removeBrackets(subjStr))
    context = new URIImpl(removeBrackets(graphStr))
    predicate = new URIImpl(removeBrackets(predStr))
    objecct = if(removeBrackets(objStr).startsWith("http"))
                new URIImpl(removeBrackets(objStr))
              else
              {
                val os = removeBrackets(objStr)
                var ind = os.indexOf("\"")+1
                val strVal =
                  if(ind > 0) os.substring(ind, os.indexOf("\"", ind)) else os
                ind = os.indexOf("@")+1
                val lang: String = if(ind > 0) os.substring(ind).trim else null
                ind = os.indexOf("<")+1
                val datatype: URI = if(ind > 0) new URIImpl(os.substring(ind, os.indexOf(">", ind))) else null

                if(lang != null)
                  new LiteralImpl(strVal, lang)
                else if(datatype != null)
                  new LiteralImpl(strVal, datatype)
                else
                  new LiteralImpl(strVal)
              }

  }

  private def removeBrackets(uri: String): String =
  {
    if(uri.trim.startsWith("<"))
      uri.replace("<", "").replace(">", "").trim
    else
      uri
  }


  override def getContext: Resource = context

  override def getSubject: Resource = subject

  override def getPredicate: URI = predicate

  override def getObject: Value = objecct

  override def toString: String = NQuadStatement.statementToNQuadString(this)
}

object NQuadStatement{
  def statementToNQuadString(st: Statement): String =
  {
    var str = "<" + st.getSubject.stringValue() + ">\t<" + st.getPredicate.stringValue() + ">\t"
    if(st.getObject.isInstanceOf[URI] || st.getObject.isInstanceOf[BNode])
     str +=  "<" + st.getObject.stringValue() + ">\t"
    else //Literal
    {
      val lit = st.getObject.asInstanceOf[Literal]
      str +=  "\"" + lit.getLabel.replace("\"", "\\\"") + "\""
      lit.getLanguage match {
        case lang:String => str += "@" + lang
        case null =>
      }
      lit.getDatatype match {
        case t:URI => str += "^^<" + t.stringValue() + ">"
        case null =>
      }
    }
    if(st.getContext != null)
      str += "\t<" + st.getContext.stringValue() + ">"
    str + " .\n"
  }
}
