package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` if the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    def bal(n: Int, idx: Int): Boolean = {
      if (idx == chars.size) return n == 0
      def c = chars(idx)
      if (c == '(') bal(n + 1, idx + 1)
      else if (c == ')') {
        if (n == 0) false else bal(n - 1, idx + 1)
      }
      else bal(n, idx + 1)
    }
    bal(0, 0)
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, open: Int, close: Int): (Int, Int) = {
      if (idx == until - 1) (open, close)
      else {
        val c = chars(idx)
        if (c == '(') traverse(idx + 1, until, open + 1, close)
        else if (c == ')') {
          if (open > 0) traverse(idx + 1, until, open - 1, close)
          else traverse(idx + 1, until, open, close + 1)
        }
        else traverse(idx + 1, until, open, close)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until <= from) (0, 0)
      else if (until - from > threshold) {
        val mid = from + (until - from) / 2
        val (a, b) = parallel(reduce(from, mid), reduce(mid, until))
        (a._1 + b._1 - a._2 - b._2, 0)
      }
      else traverse(from, until, 0, 0)
    }

    //if (chars.size <= threshold) balance(chars)
    reduce(0, chars.length)._1 == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
