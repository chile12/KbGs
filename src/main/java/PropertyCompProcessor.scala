import Main.{StartProcess, Finalize, DoComparisonFor, WriterStart}
import akka.actor.{Actor, ActorRef, Props}


/**
 * Created by Chile on 8/26/2015.
 */
class PropertyCompProcessor(evalWriter: ActorRef)  extends Actor with InstanceProcessor[Unit]{
  private val workers = context.actorOf(Props(classOf[InstancePropertyComparison]).withRouter(new akka.routing.SmallestMailboxRouter(6)))
  private var inputEmpty = false
  override def startProcess(): Unit =
  {
    evalWriter ! WriterStart("propProc", self.path.name)
    val instanceReader = new InstanceReader[Unit](Main.config.outFile)
    while(instanceReader.notFinished())
      instanceReader.readSubject(evaluate, action)
    inputEmpty = true
  }

  override def evaluate(input: StringBuilder): Unit =
  {
    if(input.getClass() == classOf[StringBuilder]) {
      val isb: StringBuilder = input.asInstanceOf[StringBuilder]
      workers ! DoComparisonFor(evalWriter, isb.toString())
    }
    else
      Main.logger.warning("Actor " + self.path.name + ": evaluate function was not called with a StringBuilder")

  }

  override def action(evalResult: Unit): Unit =
  {
    if(inputEmpty) {
      evalWriter ! Finalize
    }
  }

  override def finish(): Unit =
  {

  }

  override def receive: Receive =
  {
    case StartProcess =>
      startProcess
    case _ =>
  }

}
