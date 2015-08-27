package org.aksw.kbgs.inout

/**
 * Created by Chile on 8/23/2015.
 */
class DoubleBufferedIterator[T](itr : Iterator[T]) extends BufferedIterator[T]{
  
  var h : T = itr.next()
  var c : T = null.asInstanceOf[T]
  
  override def head: T = h

  override def next(): T = 
  {
    c = h
    if(itr.hasNext)
      h = itr.next()
    else
      h = null.asInstanceOf[T]
    c
  }

  override def hasNext: Boolean = 
  {
    if(h == null)
      false
    else
      true
  }
  
  def current: T = c
}
