package org.aksw.kbgs.inout

import scala.collection.mutable


/**
 * Created by Chile on 8/27/2015.
 */
class PagedFifoStack[A](size: Int, more: () => A) {

  private var que = new mutable.Queue[A]()
  private var a: A = more()

  for(i <- 0 until size)    next()

  def update(idx: Int, elem: A): Unit =
  {
      que.update(idx, elem)
  }

  def length: Int =
  {
    que.size
  }

  def apply(idx: Int): A =
  {
    que(idx)
  }

  def pop: A =
  {
    next()
    que.dequeue()
  }

  def top : A =
  {
    que.front
  }
  def clear() : scala.Unit =
  {
    que.clear()
  }

  private def next(): Unit =
  {
    que.enqueue(a)
    a = more()
  }
  def isEmpty : scala.Boolean = {
    que.size <= 0
  }

  def iterator: Iterator[A] = {
    new Iterator[A] {
      override def hasNext: Boolean = a != null

      override def next(): A = {
        val r = a
        a = more()
        r
      }
    }
  }
}
