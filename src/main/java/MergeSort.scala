/**
 * Created by Chile on 8/25/2015.
 * pilfered from: http://blog.yunglinho.com/blog/2013/03/19/parallel-external-merge-sort/
 */

import java.io._
import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

class MergeSort {

  /**
   * limits number of reading and sorting can be executed simultaneously. Because
   * this is an IO bound operation, unless the inputstream is coming from a slow
   * http connection, otherwise, 5 is more than enough.
   */
  private val GLOBAL_THREAD_LIMIT = {
    val ret = Runtime.getRuntime.availableProcessors() / 2
    if (ret > 5) {
      6
    } else {
      ret
    }
  }

  private lazy implicit val executionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(GLOBAL_THREAD_LIMIT))

  def sort(path: String, outputFile: String): Future[File] = sort(new FileInputStream(path), outputFile)

  def sort(inputStream: InputStream, outputFile: String, chunkSize: Int = 2000000): Future[File] = {

    // open source stream
    val soure = Source.fromInputStream(inputStream).getLines().toStream
    val linesStream = lift(soure, chunkSize)
    val chunkCounter = new AtomicInteger(0)

    val outFile: Path = Paths.get(outputFile)

    val sortedFileDir = outFile.getParent

    // read source stream, read n entries into memory and save it to file in parallel.
    val saveTmpFiles: Future[List[File]] = Future.sequence(
      linesStream.map(s => {
        val chunk = chunkCounter.getAndIncrement
        Future {
          val sorted = s.sorted
          val ret = new File(outFile.toAbsolutePath.toString)
          val zip = new GZIPOutputStream(new FileOutputStream(ret))
          val out = new PrintWriter(new OutputStreamWriter(zip, "UTF-8"))

          try {
            sorted.foreach(out.println(_))
          } finally {
            out.close()
          }
          ret
        }
      }).toList
    )

    // perform merge sort.
    saveTmpFiles.map {
      files => {
        var merged = files
        while (merged.length > 1) {
          val splited = merged.splitAt(merged.length / 2)
          val tuple = splited._1.zip(splited._2)

          val m2 = tuple.map {
            case (f1, f2) => {
              val ret = new File(sortedFileDir.toString, f1.getName + "-" + f2.getName)

              val source1 = Source.fromFile(f1)
              val source2 = Source.fromFile(f2)
              val out = new PrintWriter(ret)

              try {
                val stream1 = source1.getLines().toStream.map(_.toInt)
                val stream2 = source2.getLines().toStream.map(_.toInt)
                merge(stream1, stream2).foreach(out.println(_))
                ret
              } finally {
                out.close()
                source1.close()
                source2.close()

                //TODO delete quietly?
                try {
                  f1.deleteOnExit()
                  f2.deleteOnExit()
                }
                catch {
                  case _ =>
                }
              }

            }
          }
          merged = if (merged.length % 2 > 0) {
            m2 :+ merged.last
          } else {
            m2
          }
        }
        merged.head
      }
    }
  }

  /**
   * Lift a Stream into a Stream of Stream. The size of each sub-stream is specified
   * by the chunkSize.
   *
   * @param stream        the origin stream.
   * @param chunkSize     the size of each substream
   * @tparam A
   * @return              chunked stream of the original stream.
   */
  private def lift[A](stream: Stream[A], chunkSize: Int): Stream[Stream[A]] = {

    def tailFn(remaining: Stream[A]): Stream[Stream[A]] = {
      if (remaining.isEmpty) {
        Stream.empty
      } else {
        val (head, tail) = remaining.splitAt(chunkSize)
        Stream.cons(head, tailFn(tail))
      }
    }
    val (head, tail) = stream.splitAt(chunkSize)
    return Stream.cons(head, tailFn(tail))
  }


  /**
   * Merge two streams into one stream.
   * @param streamA
   * @param streamB
   * @return
   */
  private def merge[A](streamA: Stream[A], streamB: Stream[A])(implicit ord: Ordering[A]) : Stream[A] = {

    (streamA, streamB) match {
      case (Stream.Empty, Stream.Empty) => Stream.Empty
      case (a, Stream.Empty) => a
      case (Stream.Empty, b) => b
      case _ => {
        val a = streamA.head
        val b = streamB.head

        if (ord.compare(a, b) > 0) {
          Stream.cons(a, merge(streamA.tail, streamB))
        } else {
          Stream.cons(b, merge(streamA, streamB.tail))
        }
      }
    }
  }
}
