package org.aksw.kbgs.helpers

import com.google.common.collect.HashMultimap
import org.aksw.kbgs.Main

import scala.collection.JavaConverters._

/**
 * Created by Chile on 8/28/2015.
 */
class IdBuffer(sourceFile: String, bufferSize: Int) {
  private val source = Main.getSource(sourceFile).getLines()
  private val buffer = HashMultimap.create[String, String]()
  fillBuffer()
  
  private def addFromResource() {
    val inp: String = source.next
    val uri: String = inp.substring(0, inp.indexOf(" - "))
    val id: String = inp.substring(inp.indexOf(" - ") + 3)
    buffer.put(uri, id)
  }

  def remove(uri: String) {
    buffer.removeAll(uri)
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
      buffer.get(key).asScala.toList
  }

  def getKeys: List[String] = {
    buffer.keySet.asScala.toList
  }

  def contains(key: String): Boolean = {
    buffer.keySet().contains(key)
  }

  def size: Int = {
    buffer.size
}

  def getMap(): HashMultimap[String, String] =
  {
    buffer
  }
}
