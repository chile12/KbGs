package org.aksw.kbgs

import java.io._
import java.util.logging.{Level, Logger}
import java.util.zip.GZIPInputStream

import akka.actor.{ActorSystem, Props}
import org.aksw.kbgs.Contractor.StartProcess
import org.aksw.kbgs.helpers.ConfigImpl

import scala.io.BufferedSource

/**
 * Created by Chile on 8/23/2015.
 */
object Main {
  val logger = Logger.getLogger("")
  logger.setLevel(Level.ALL)
  var config: ConfigImpl = null

  def main(args: Array[String]) {
    assert((args.length == 1))
    val actorSystem = ActorSystem()
    config = new ConfigImpl(args(0))
    val distributor = actorSystem.actorOf(Props(classOf[Distributor]), "distributor")
    distributor ! StartProcess
    //distributor ! StartSecondPass
  }

  def getSource(path: String) : BufferedSource =
  {
    if(path.trim().endsWith(".gz"))
      new BufferedSource(getInputStream(path))
    else
      new BufferedSource(getInputStream(path))
  }

  def getInputStream(path: String) : InputStream =
  {
    if(path.trim().endsWith(".gz"))
      new GZIPInputStream(new BufferedInputStream(new FileInputStream(path)))
    else
      new BufferedInputStream(new FileInputStream(path))
  }

}
