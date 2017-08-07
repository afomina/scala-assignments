package quickcheck

import java.util.NoSuchElementException

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = lzy(

    for {
      h <- oneOf(const(empty), genHeap)
      a <- arbitrary[Int]
    } yield insert(a, h)
  )

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("gen2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, empty)
    findMin(insert(b, h)) == Math.min(a, b)
  }

  property("gen3") = forAll { (a: Int) =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }

  property("gen4") = forAll { (h: H) =>
    val list = findDel(h)
    list == list.sorted
  }

  def findDel(h: H): List[Int] = {
    if (isEmpty(h)) Nil
    else findMin(h) :: findDel(deleteMin(h))
  }

  property("gen5") = forAll { (h: H, q: H) =>
    val mel = meld(h, q)
    val min = findMin(mel)
    min == Math.min(findMin(h), findMin(q))
  }

  property("gen6") = forAll { (h: H) =>
    findDel(deleteMin(h)).size == findDel(h).size - 1
  }

  property("gen7") = forAll { (h: H) =>
    val m = findMin(h)
    val v = if (m == Int.MinValue) m else m-1
    val h1 = insert(v, h)
    findMin(h1) == Math.min(v, m)
  }

  property("gen8") = forAll { (h: H) =>
    val m = findMin(h)
    val v = if (m == Int.MaxValue) m else m+1
    val h1 = insert(v, h)
    findMin(h1) == m
  }

  property("gen9") = forAll { (h: H, q: H) =>
    findDel(meld(h, q)).size == findDel(h).size + findDel(q).size
  }

  property("gen10") = forAll { (h: H) =>
    findDel(insert(1, h)).size == findDel(h).size + 1
  }
}
