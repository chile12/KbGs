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
  private var offset =0
  for(s <- sourcePaths)
  {
    val zw = Main.getSource(s).getLines()
    source.add(zw)
    System.out.println("reading file: " + s)
  }
  private var lastRead: String = null
  private var finished = false
  readIntern()  //get first line

  override def next(): Option[StringBuilder] =
  {
    if(source.isEmpty)
      return None
    if(lastRead.replaceAll("(?m)^[\\s]*#.*$", "").trim.length == 0)
    {
      readIntern()
      return next()
    }
    val sb = new StringBuilder()
    val subject = lastRead.substring(0, lastRead.indexOf(">")+1)
    if(subject == "")
      throw new RDFParseException("source file " + sourcePaths + " is not in a valid nt-rdf serialization!")
    sb.append(lastRead)
    while (readIntern().startsWith(subject)) {
      sb.append(lastRead)
    }
    Option(sb)
  }

  private def readIntern(): String =
  {
    if(source.size() > 0) {
      while (source.get(0).isEmpty) {
        source.remove(0)
        if (source.size() == 0) {
          finished = true
          lastRead = ""
          return ""
        }
      }
      if (source.get(0).hasNext)
      {
        lastRead = source.get(0).next().trim + "\n"
        offset = 0
      }
      return lastRead
    }
    ""
  }


  override def hasNext: Boolean = !finished

  override def close(): Unit =
  {
  }

  override def read(cbuf: Array[Char], off: Int, len: Int): Int =
  {
    for(i <- 0 until len)
    {
      getNectChar() match {
        case Some(x) => cbuf.update(i + off, x)
        case None => return i
      }
    }
    len
  }

  private def getNectChar(): Option[Char] =
  {
    offset += 1
    if(lastRead.length >= offset)
      return Option(lastRead(offset-1))
    else
    {
      readIntern()
      if(lastRead.length >= offset)
        return Option(lastRead(offset-1))
      None
    }
  }
}
