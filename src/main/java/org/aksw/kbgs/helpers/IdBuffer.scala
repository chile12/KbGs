package org.aksw.kbgs.helpers

import org.aksw.kbgs.Main

import scala.collection.mutable.{HashMap, MultiMap, Set}

/**
 * Created by Chile on 8/28/2015.
 */
class IdBuffer(sourceFile: String, bufferSize: Int) {
  private val source = Main.getSource(sourceFile).getLines()
  private val buffer = new HashMap[String, Set[String]] with MultiMap[String, String]
  fillBuffer()
  
  private def addFromResource() {
    val inp: String = source.next
    val uri: String = inp.substring(0, inp.indexOf(" - "))
    val id: String = inp.substring(inp.indexOf(" - ") + 3)
    buffer.addBinding(uri, id)
  }

  def remove(uri: String) {
    buffer.remove(uri)
    fillBuffer()
  }
  
  private def fillBuffer(): Unit=
  {
    if(bufferSize < 0)
      while(source.hasNext)
        addFromResource()
    else
      for(i <- buffer.size until bufferSize)
        if (source.hasNext)
          addFromResource()
  }

  def getValues(key: String): List[String] = {
    if(buffer.get(key).get != null)
      buffer.get(key).get.toList
    else
      null
  }

  def getKeys: List[String] = {
    buffer.keySet.toList
  }

  def contains(key: String): Boolean = {
    buffer.contains(key)
  }

  def size: Int = {
    buffer.size
}

  def getMap(): HashMap[String, Set[String]] =
  {
    buffer.clone()
  }
}
