package com.cibo.provenance


import com.cibo.io.s3.SyncablePath
import org.scalatest.{FunSpec, Matchers}
import com.cibo.aws.AWSClient.Implicits.s3SyncClient
import com.cibo.io.s3.SyncablePathBaseDir.Implicits.default

class ReadmeSpec extends FunSpec with Matchers {
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  implicit val buildInfo: BuildInfo = BuildInfoDummy

  describe("the short example in the README") {
    it("runs") {
      val testSubdir = f"readme-short"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      myApp1.main(Array[String](testDataDir))
    }
  }

  describe("the long example in the README") {
    it("runs") {
      val testSubdir = f"readme-long"
      val testDataDir = f"$testOutputBaseDir/$testSubdir"
      implicit val rt = ResultTrackerForSelfTest(testDataDir)
      rt.wipe

      myApp2.main(Array[String](testDataDir))
    }
  }
}

// Code from the Shorter Example in the README.  Always edit it here and paste it there.

object myMkString extends Function2WithProvenance[Int, Double, String] {
  val currentVersion = Version("0.1")
  def impl(i: Int, d: Double): String = i.toString + ":" + d.toString
}

object myStrLen extends Function1WithProvenance[String, Int] {
  val currentVersion = Version("0.1")
  def impl(s: String): Int = s.length
}

object myApp1 extends App {
  implicit val bi: BuildInfo = com.cibo.provenance.BuildInfoDummy // use com.mycompany.myapp.BuildInfo

  // This is what is in the README.
  //implicit val rt: ResultTracker = ResultTrackerForTest(args(0))   //"s3://mybucket/myroot")

  // This is an alternate actually in the test suite.
  val testOutputBaseDir: String = TestUtils.testOutputBaseDir
  val testSubdir = "readme-long"
  val testDataDir = f"$testOutputBaseDir/$testSubdir"
  implicit val rt = ResultTrackerForSelfTest(testDataDir)
  rt.wipe

  import io.circe.generic.auto._

  val c1: myMkString.Call = myMkString(3, 1.23)
  val r1: myMkString.Result = c1.resolve
  val s1: String = r1.output // "3:1.23"

  val c2: myStrLen.Call = myStrLen(r1)
  val r2: myStrLen.Result = c2.resolve
  val s2: Int = r2.output // 5

  val c3: myMkString.Call = myMkString(r2, 7.89)
  val r3: myMkString.Result = c3.resolve
  val s3: String = r3.output // "5:7.89"

  assert(r3.call.unresolve == myMkString(myStrLen(myMkString(3, 1.23)), 7.89))
}


// Code from the Longer Example in the README.  Always edit it here, and paste it there.

object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int): Int = a + b
}

object myApp2 extends App {

  implicit val bi: BuildInfo = BuildInfoDummy
  implicit val rt: ResultTracker = ResultTrackerForSelfTest(args(0)) // or s3://...

  import io.circe.generic.auto._

  // Basic use: separate objects to represent the logical call and the result and the actual output.
  val call1 = addMe(2, 3)             // no work is done
  rt.hasOutputForCall(call1)          // false (unless someone else did this)

  val result1 = call1.resolve         // generate a result, if it does not already exist
  println(result1.output)             // get the output: 5
  println(result1.call)               // get the provenance: call1
  rt.hasOutputForCall(call1)          // true (now saved)


  val result1b = call1.resolve        // finds the previous result, doesn't run anything
  result1b == result1

  // Nest:
  val call2 = addMe(2, addMe(1, 2))
  val result2 = call2.resolve         // adds 1+2, but is lazy about adding 2+3 since it already did that
  result2.output == result1.output    // same output
  result2.call != result1.call        // different provenance

  // Compose arbitrarily:
  val bigPlan = addMe(addMe(6, addMe(result2, call1)), addMe(result1, 10))

  // Don't repeat any call with the same inputs even with different provenance:
  val call3: addMe.Call = addMe(1, 1)               // (? <- addMe(raw(1), raw(1)))
  val call4: addMe.Call = addMe(2, 1)               // (? <- addMe(raw(2), raw(1)))
  val call5: addMe.Call = addMe(call3, call4)       // (? <- addMe(addMe(raw(1), raw(1)), addMe(raw(2), raw(1))))
  val result5 = call5.resolve                       // runs 1+1, then 2+1, but shortcuts past running 2+3 because we r1 was saved above.
  assert(result5.output == result1.output)          // same answer
  assert(result5.call != result1.call)              // different provenance

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


object trioToList extends Function3WithProvenance[Int, Int, Int, List[Int]] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int, c: Int): List[Int] = List(a, b, c)
}

object incrementMe extends Function1WithProvenance[Int, Int] {
  val currentVersion = Version("0.1")
  def impl(x: Int): Int = x + 1
}

object sumList extends Function1WithProvenance[List[Int], Int] {
  val currentVersion = Version("0.1")
  def impl(lst: List[Int]): Int = lst.sum
}
