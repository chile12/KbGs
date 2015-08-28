package org.aksw.kbgs.inout

import org.aksw.kbgs.Main
import org.openrdf.rio.RDFParseException

/**
 * Created by Chile on 8/25/2015.
 * 
 * used for reading the whole instance of a resource from a sorted ntriple file
 */
class InstanceReader[T](sourcePath: String) {

  private val source = Main.getSource(sourcePath).getLines()
  private var lastRead: String = null
  private var finished = false
  read()  //get first line

  /**
   * creates and sends the full instance (with all (mapped) properties, defined by the uri to the outputWriter
   * @return uri of the completed instance
   */
  def readSubject(evalFunction: (StringBuilder) => T, resultFunction:(T) => Unit): Unit=
  {
    while(source.hasNext)
    {
      val retVal = evalFunction(readNextSubject)
      if(retVal != null && resultFunction != null) {
        resultFunction(retVal)
        return
      }
    }
    finished = true
  }

  def readNextSubject(): StringBuilder =
  {
    if(source.isEmpty)
      return null
    val sb = new StringBuilder()
    val subject = lastRead.substring(0, lastRead.indexOf(">")+1)
    if(subject == "")
      throw new RDFParseException("source file " + sourcePath + " is not in a valid nt-rdf serialization!")
    sb.append(lastRead)
    while (read().startsWith(subject)) {
      sb.append(lastRead)
    }
    sb
  }

  def pagesSeqLoader(fill: Array[Object], start: Int, end: Int): Int =
  {
    var r = -1
    for(i <- start until end)
      if(source.isEmpty){
        if(r < 0)
          r+=1
        fill(i) = readNextSubject()
        r+=1
      }
    r
  }

  private def read(): String =
  {
    if(source.hasNext)
      lastRead = source.next().trim + "\n"
    else {
      finished = true
      lastRead = ""
    }
    lastRead
  }

  def notFinished(): Boolean =
  {
    !finished
  }
}
