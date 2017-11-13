Data Provenance
===============

Example:
--------

```scala

import com.cibo.provenance._

package com.cibo.provenance.examples

// Make this instead of `def addMe(a: Int, b: Int): Int = a + b`.
// Don't make real functions with provenance that are this granular. :)

object addMe extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int): Int = a + b
}

object MyApp extends App {
    implicit val bi: BuildInfo = BuildInfo
    
    implicit val rt: ResultTracker = ResultTrackerSimple("/tmp/mydata") // or s3://...
    
    // Basic use: separa
    val call1 = addMe(2, 3)             // no work is done
    val result1 = call1.run()           // generate a result
    println(r1.output)                  // get the output: 5
    println(r1.provenance)              // geth the provenance: call1
    
    rt.hasResult(call1)                 // false (unless sonmeone else did this)
    
    // Check a db for a result, and run only if necessary:
    result1b = call1.resolve            // grabs the implicit rt
    result1b == result1                 // makes a similar result
    rt.hasResult(call1)                 // true (now saved)

    // Nest:
    val call2 = addMe(2, addMe(1, 2)    
    val result2 = call2.resolve                 // adds 1+2, but is lazy about adding 2+3 since it already did that
    result2.output == result1.output            // same output
    result2.provenance != result1.provenance    // different provenance

    // Compose arbitarily:
    val bigPlan = addMe(addMe(addMe(2, 2), addMe(10, addMe(r1, c1)), addMe(5, 5))
    
    // Don't repeat any call with the same inputs even with different provenance:
    c2 = addMe(1, 1)                    // (? <- addMe(raw(1), raw(1)))
    c3 = addMe(2, 1)                    // (? <- addMe(raw(2), raw(1)))
    c4 = addMe(c2, c3)                  // (? <- addMe(addMe(raw(1), raw(1)), addMe(raw(2), raw(1)))
    c4.resolve()                        // runs 1+1, then 2+1, but shortcuts past running 2+3 because we r1 was saved above.
    assert(c4.output == c1.output)      // same answer
    assert(c4.provenance != c1)         // different provenance

    // Builtin Functions for Map, Apply, etc.
    
    // Track a list as a single thing.
    val lst: trioToList.Call = trioToList(10, 20, 30)
    
    // The apply method works direclty 
    // Dip into results that are sequences and pull out individual values, but keep provenance
    val call3 = addMe(lst(0), lst(1)).resolve.output == 30
    val call4 = addMe(lst(1), lst(2)).resolve.output == 50

    // Make a result that is a List with provenance, and extract individual alues with provenance.
    val lst = trioToList(1, 2, 3, 4, 5, 6, 7)       // FunctionCallWWithProvenance[List[Int]]
    val item = lst(2)                               // ApplyWithProvenance[Int, List[Int], Int]
    item.resolve.output == 3

    // Map over functions with provenance tracking.
    val lst2 = lst.map(incrementMe).map(incrementMe)    // The .map method is also added.
    lst2(2) == 5                                        // And .apply works on the resultint maps.
    
    // Or pass the full result of map in.
    val bigCall2 = sumList(lst.map(incrementMe).map(incrementMe))
    val bigResult2 = bigCall2.resolve
    bigResult2.output == 66

}

object trioToList extends Function4WithProvenance[List[Int], Int, Int, Int] {
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

```


Overview:
---------

A regular function might be defined like this:
```scala
def foo(i: Int, d: Double): String =
    i.toString + "," + d.toString
```

More verbose:
```scala
object foo extends Function2[String, Int, Double] {
    def apply(i: Int, d: Double): String =
        i.toString + "," + d.toString
}
```

Add data provenance tracking:
```scala
object foo extends Function2WithProvenance[String, Int, Double] {
    val currentVersion = Version("0.1")
    def impl(i: Int, d: Double): String =
        i.toString + "," + d.toString
}
```

Use it:
```scala
val f: foo.Call     = foo(5, 1.23)
val r: foo.Result   = f.run()
val o: String       = r.output
```

The result knows where the output value came from:
```scala
val f2: foo.Call    = r.provenance
assert(f2 == f)
```

Save it:
```scala
val db = ResultTrackerSimple("/my/datarepo")
db.saveResult(r)
```

Or see if someone else already did it:
```scala
val r2: Option[foo.Result] = db.getResultOption(foo(5, 1.23))
```

Compose:
```scala
object addInts extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int) = a + b
}

implicit db = ResultTrackerSimple("/my/data")

val s1 = addInts(10, addInts(6, 6))
val r1 = s1.resolve()
// ^^ calls 6+6 and 10+12

val s2 = addInts(10, addInts(5, 7))
val r2 = s2.resolve()
// ^^ calls 5+7, but skips calling 10+12

val s3 = addInts(5, addInts(5, addInts(3, 4))
val r3 = s3.resolve()
// ^^ calls 3+4, but skips 5+7 and 10+12 
```







