import java.io.StringReader

import Main.{DoComparisonFor, AddCompResult}
import akka.actor.{Actor, ActorRef}
import com.google.common.collect.HashBasedTable
import org.openrdf.model.Model
import org.openrdf.model.impl.{TreeModel, URIImpl}
import org.openrdf.query.algebra.evaluation.util.ValueComparator
import org.openrdf.rio.{RDFFormat, Rio}

import scala.collection.mutable.HashMap

/**
 * Created by Chile on 8/26/2015.
 */
class InstancePropertyComparison() extends Actor{

  if (InstancePropertyComparison.modelMap.size == 0)
    InstancePropertyComparison.fillModelMap()
  val modelMap = InstancePropertyComparison.modelMap.clone()
  val compTable = HashBasedTable.create[String, String, HashMap[String, Int]]()

  private var compdone = false


  def doCompare(instance: String): Unit = {
    if(instance == null)
      Main.logger.warning("Could not send eval result due to missing input. Actor: " + self.path)
    val sr = new StringReader(instance)
    val parser = Rio.createParser(RDFFormat.NQUADS)
    parser.setRDFHandler(new MultiContextHandler(modelMap))
    parser.parse(sr, "<http://aksw.org/kbgs/isbn/>")

    val comparator = new ValueComparator()
    for (kb1 <- modelMap.keySet) {
      for (kb2 <- modelMap.keySet) {
        if(kb1 != kb2) {
          for (prop <- Main.config.properties) {
            val valueMap = new HashMap[String, Int]()
            val valSelector1 = modelMap.get(kb1).get.filter(null, new URIImpl(prop._1), null).objects()
            val val1 = if(valSelector1.size() > 0) valSelector1.iterator().next() else null
            val valSelector2 = modelMap.get(kb2).get.filter(null, new URIImpl(prop._1), null).objects()
            val val2 = if(valSelector2.size() > 0) valSelector2.iterator().next() else null
            val result = comparator.compare(val1, val2)
            valueMap.put(prop._1, result)
            compTable.put(kb1, kb2, valueMap)
          }
        }
      }
    }
    compdone = true
  }

  def sendResultsTo(writer: ActorRef, input: String): Unit =
  {
    if(!compdone)
      doCompare(input)
    for (kb1 <- modelMap.keySet) {
      for (kb2 <- modelMap.keySet) {
        if (kb1 != kb2) {
          for (prop <- Main.config.properties) {
            writer ! AddCompResult(kb1, kb2, prop._1, getResultFor(kb1, kb2, prop._1))
          }
        }
      }
    }
  }

  def getResultFor(kb1: String, kb2: String, property: String): (Int, Int) = {

    val valueMap = compTable.get(kb1, kb2)
    if(property != null)
      return (valueMap.get(property).get, 1)
    evalPropTable(valueMap)
  }


  private def evalPropTable(compTable: HashMap[String, Int]): (Int, Int) = {
    var all = 0
    var hit = 0
    for (prop <- compTable)
    {
      hit += prop._2
      all += 1
    }
    (hit, all)
  }

  override def receive: Receive =
  {
    case DoComparisonFor(writer, input) =>
    {
      sendResultsTo(writer, input)
    }
    case _ =>
  }
}

object InstancePropertyComparison{
  private val modelMap: HashMap[String, Model] = new HashMap[String, Model]()

  private def fillModelMap(): Unit =
  {
    for(prop <- Main.config.kbMap) {
      if (modelMap.get(prop._2.get("kbGraph").get) == None)
        modelMap.put(prop._2.get("kbGraph").get, new TreeModel())
    }
  }
}
