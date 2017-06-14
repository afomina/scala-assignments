package funsets

object Main extends App {
  import FunSets._
  println(contains(singletonSet(1), 1))
  printSet( (x:Int) => x > 0 && x <= 25)
}
