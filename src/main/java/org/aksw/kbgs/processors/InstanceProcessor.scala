package org.aksw.kbgs.processors

import scala.concurrent.Future

/**
 * Created by Chile on 8/25/2015.
 */
trait InstanceProcessor[I, T] {
  def startProcess(): Unit
  def evaluate(input: I): Future[T]
  def action(evalResult: T): Unit
  def finish(): Unit
//  def workForceSize: Int
//  def workerParameter: Array[Any]
//  def workerType: ClassTag[_]
}
