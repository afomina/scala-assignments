package recfun

object Main {
  def main(args: Array[String]) {
    println(countChange(4, List(1,2)))
//    print(balance("(((hello)there)you)(".toList))
//        println("Pascal's Triangle")
//        for (row <- 0 to 10) {
//          for (col <- 0 to row)
//            print(pascal(col, row) + " ")
//          println()
//        }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int =
    if (r == 0 || c == 0 || c == r) 1
    else if (c > r) 0
    else pascal(c-1 , r-1) + pascal(c, r-1)


  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def bal(chars: List[Char], n: Int): Boolean = {
      if (chars.isEmpty) return n == 0
      def c = chars.head
      if (c == '(') bal(chars.tail, n+1)
      else if (c == ')') {
        if (n == 0) false else bal(chars.tail, n-1)
      }
      else bal(chars.tail, n)
    }

    bal(chars, 0)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0 || coins.isEmpty) return 0
    def coin = coins.head
    if (coin == money)
      1 + countChange(money, coins.tail)
    else if (coin < money)
      countChange(money - coin, coins) + countChange(money, coins.tail)
    else countChange(money, coins.tail)
  }
}