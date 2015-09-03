package org.aksw.kbgs.inout

import java.util

import org.aksw.kbgs.Main
import org.openrdf.rio.RDFParseException

/**
 * Created by Chile on 8/25/2015.
 * 
 * used for reading the whole instance of a resource from a sorted ntriple file
 */
class InstanceReader(sourcePaths: List[String]) extends WorkLoader[StringBuilder]{

  private var source = new util.ArrayList[Iterator[String]]()
  for(s <- sourcePaths)
  {
    val zw = Main.getSource(s).getLines()
    source.add(zw)

  }
  private var lastRead: String = null
  private var finished = false
  read()  //get first line

  override def next(): StringBuilder =
  {
    if(source.isEmpty)
      return null
    if(lastRead.replaceAll("(?m)^[\\s]*#.*$", "").trim.length == 0)
    {
      read()
      return next()
    }
    val sb = new StringBuilder()
    val subject = lastRead.substring(0, lastRead.indexOf(">")+1)
    if(subject == "")
      throw new RDFParseException("source file " + sourcePaths + " is not in a valid nt-rdf serialization!")
    sb.append(lastRead)
    while (read().startsWith(subject)) {
      sb.append(lastRead)
    }
    sb
  }
  private def read(): String =
  {
    if(source.size() > 0) {
      while (source.get(0).isEmpty) {
        source.remove(0)
        if (source.size() == 0)
          finished = true
        lastRead = ""
        return ""
      }
      if (source.get(0).hasNext)
        lastRead = source.get(0).next().trim + "\n"
      return lastRead
    }
    ""
  }

  override def hasNext: Boolean = !finished
}
