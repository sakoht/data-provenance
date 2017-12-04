package com.cibo.provenance


import org.scalatest.{FunSpec, Matchers}

class ReadmeSpec extends FunSpec with Matchers {
  import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerNone}
  import org.slf4j.{Logger, LoggerFactory}

  describe("code in the README") {
    it("runs") {
      MyApp.main(Array[String]())
    }
  }

}

// Example code from the README.  Always edit it here, and place it there.

import com.cibo.provenance._
import com.cibo.provenance.tracker.{ResultTracker, ResultTrackerSimple}


object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int): Int = a + b
}

object MyApp extends App {

  implicit val bi: BuildInfo = DummyBuildInfo
  implicit val rt: ResultTracker = ResultTrackerSimple("/tmp/mydata") // or s3://...

  // Basic use: separate objects to represent the logical call and the result and the actual output.
  val call1 = addMe(2, 3)             // no work is done
  rt.hasResultForCall(call1)          // false (unless someone else did this)

  val result1 = call1.resolve         // generate a result, if it does not already exist
  println(result1.output)             // get the output: 5
  println(result1.provenance)         // get the provenance: call1
  rt.hasResultForCall(call1)          // true (now saved)


  val result1b = call1.resolve        // finds the previous result, doesn't run anything
  result1b == result1

  // Nest:
  val call2 = addMe(2, addMe(1, 2))
  val result2 = call2.resolve                       // adds 1+2, but is lazy about adding 2+3 since it already did that
  result2.output == result1.output                  // same output
  result2.provenance != result1.provenance          // different provenance

  // Compose arbitrarily:
  val bigPlan = addMe(addMe(6, addMe(result2, call1)), addMe(result1, 10))

  // Don't repeat any call with the same inputs even with different provenance:
  val call3: addMe.Call = addMe(1, 1)               // (? <- addMe(raw(1), raw(1)))
  val call4: addMe.Call = addMe(2, 1)               // (? <- addMe(raw(2), raw(1)))
  val call5: addMe.Call = addMe(call3, call4)       // (? <- addMe(addMe(raw(1), raw(1)), addMe(raw(2), raw(1))))
  val result5 = call5.resolve                       // runs 1+1, then 2+1, but shortcuts past running 2+3 because we r1 was saved above.
  assert(result5.output == result1.output)          // same answer
  assert(result5.provenance != result1.provenance)  // different provenance

  // Builtin Functions for Map, Apply, etc.

  val list1 = UnknownProvenance(List(10, 20, 30))
  val list2 = list1.map(incrementMe)
  val e1 = list2(2)
  val r1 = e1.resolve

  assert(r1.output == 31)

  // The apply method works directly.
  // Dip into results that are sequences and pull out individual values, but keep provenance.
  val call6 = addMe(list1(0), list1(1)).resolve.output == 30
  val call7 = addMe(list1(1), list1(2)).resolve.output == 50

  // Make a result that is a List with provenance, and extract individual alues with provenance.
  val list3  = trioToList(5, 6, 7)      // FunctionCallWWithProvenance[List[Int]]
  val item = list3(2)                   // ApplyWithProvenance[Int, List[Int], Int]
  assert(item.resolve.output == 7)

  val lstCx = list2.map(incrementMe)
  val r = lstCx.resolve

  // Map over functions with provenance tracking.
  val lstC = list2.map(incrementMe).map(incrementMe)    // The .map method is also added.
  lstC(2).resolve.output == 13                          // And .apply works on the resulting maps.

  // Or pass the full result of map in.
  val bigCall2 = sumList(list2.map(incrementMe).map(incrementMe))
  val bigResult2 = bigCall2.resolve
  bigResult2.output == 66

}

object trioToList extends Function3WithProvenance[List[Int], Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int, c: Int): List[Int] = List(a, b, c)
}

object incrementMe extends Function1WithProvenance[Int, Int] {
  val currentVersion = Version("0.1")
  def impl(x: Int): Int = x + 1
}

object sumList extends Function1WithProvenance[Int, List[Int]] {
  val currentVersion = Version("0.1")
  def impl(lst: List[Int]): Int = lst.sum
}
