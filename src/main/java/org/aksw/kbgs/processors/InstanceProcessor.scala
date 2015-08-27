package org.aksw.kbgs.processors

/**
 * Created by Chile on 8/25/2015.
 */
trait InstanceProcessor[T] {
  def startProcess(): Unit
  def evaluate(input: StringBuilder): T
  def action(evalResult: T): Unit
  def finish(): Unit
}
