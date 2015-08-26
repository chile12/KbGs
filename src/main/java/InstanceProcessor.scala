/**
 * Created by Chile on 8/25/2015.
 */
trait InstanceProcessor {
  def startProcess(): Unit
  def evaluate(input: Any): String
  def action(evalResult: String): Unit
  def finish(): Unit
}
