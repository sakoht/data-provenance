Data Provenance
===============

Background
----------


A normal scala function might be declared like this:  
```scala
def foo(a: Int, b: Double): String = a.toString + "," + b.toString
```

The above takes an `Int` and a `Double`, and returns a `String` by just concatenating the others with a comma.

A longer form of the same thing:
```scala
object foo extends Function2[String, Int, Double] {
    def apply(a: Int, b: Double): String = a.toString + "," + b.toString
}
```

Both are called the same way:
```scala
val out: String = foo(123, 9.99)
out == "123,9.99"
```


A `Function2` takes two inputs, and has three parameterized types: the output type followed by each of the input types.  Scala implements `Function0` - `Function22`.  This pattern is similar for many builtin classes (`Tuple1`-`Tuple22`, etc.).


Adding Provenance
-----------------

To add data-provenance we modify the long version of a function declaration:
- `Function2WithProvenance` replaces `Function2`
- `def impl` replaces `def apply`
- `val currentVersion: Version = Version("x.x")` is added

```scala
import com.cibo.provenance._

object foo extends Function2WithProvenance[String, Int, Double] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Double): String = a.toString + "," + b.toString
}
```

The implicit contract is:
- the function produces deterministic results for the same input at any given declared version
- the the version number will be updated when a software change intentionally change results
- the system will track enough data that, when the above fails, the system self-corrects

Using a FunctionWithProvenance
------------------------------

Applying the function doesn't actually run the implementation.  It returns a handle, similar to a Future, except the
wrapped value might be done in the past, or in another process on another machine, or never executed at all:
```scala
val call1: foo.Call = foo(123, 9.99)
```

After that, you could "resolve" the call, which will run the implementation _if_ the answer is not already stored:
```scala
val result1: foo.Result = call1.resolve     // see also resolveFuture to get back `Future[foo.Result]`
```

The result contains both the `output` and the `provenance` of the call.  That "provenance" is actually just a reference back to the call that produced it.  While the call contains the logical "version", the result contains a concrete commmit and build that was actually used to run the `impl()`.
```scala
result1.output == "123,9.99"   // the actual output of impl()
result1.provenance == call1    // the call that made it
```

Nesting
-------
A call can take raw input values, but ideally it takes the result of _other_ call that produced the input, so the provenance chain can be extended.  It can also simply take a call, and the system will convert that into a result by running it or looking up an existing answer.

An example of nesting using a toy function:
```scala
object addMe extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int): Int = a + b
}
```

Then we could:
```scala
val call1 = addMe(2, 3) 
val result1 = call1.resolve)

val call2 = addMe(7, 8) 

val call3 = addMe(addMe(addMe(2, 2), addMe(10, addMe(call1, result2)), addMe(5, 5)))
val result3: addMe.Result = call3.resolve
```

Note that, above, we used both raw values and also `result1` and `call2` as inputs.  The raw values are implicitly converted into `UnknownProvenance[T]`, a placeholder for values with no history.  All such values fall under a base 


Tracking Results
----------------

The resolver consults an implicit `ResultTracker`, which is the is an interface to storage, and selectively calling implementations in a coordinated way.  The default implementation is "broker-free", in that a central server or central locking is not required for consistency, idempotency, or concurrency.  It can handles concurrent attempts to do similar work "optimisitically": in a race condition identical work may be done, but no data is corrupted or duplicated.

```scala
implicit val rt = ResultTrackerSimple("s3://mybucket/mypath")   // can also use a local filesystem path 
rt.hasResultForCall(call1) == true                              // perhaps
val result1b = call1.resolve                                    // just loads the answer made previously
```

There is a no-op `ResultTrackerNone` that records nothing, re-runs everything.  It can becombined with the `DummyBuildInfo` to do ad-hoc experiments with no setup.
```scala
import com.cibo.provenance._
implicit val bi = DummyBuildInfo            // a dummy stub commit and build
implicit val rt = ResultTrackerNone()       // no tracking: re-run everything and save nothing
```

DRY
---

For a given result tracker, the same inputs are never passed to the same declared version of the same function.
This means that when we resolve call3 above, a result is produced for call2, since it is an input to call 3. 

A subsequent call to call2.resolve will look-up the answer rather than calculate it.

A more detailed example of "shortcutting" past calculations (or "memoizing"):

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

Versions and BuildInfo
----------------------
The version in the function is an "asserted version".  A declaration by the programmer that outputs will be consistent for the same inputs.

The call specifies the version, but with a default argument that sets it to the currentVersion.  You might create a call
with an older version for purposes of explicitly querying for old data, or inspect the version of 
a call handed to you when introspecting the provenance.

When software is behaving as intended, the version is sufficient to describe a single iteration of funciton logic.  There will be multiple repository commits, and multiple source code builds, that have the same version number for a component, because other components will also be iterating.

In theory, the versions will be updated appropriately.  In practice, errors will occur.

There are three modes:
- a function that does not really produce the same output for the same inputs repeatably
- a function tis refactored at some commit, but a change in results is introduced inadvertently
- a function that behaves differently for the same commit on differnt builds, due to some external factor in the build process

Each actual output produced tracks the exact git commit and build ID used to produce it.  This comes from SbtBuildInfo.  Each
project that uses the data-provenance library should use the `buildinfo.sbt` from the example repo to make this data available.  The BuildInfo created must be implicitly available to make a ResultTracker.

The system can detects the three above failure modes retroactively, as the developer "posts evidence" to a future test suite,
which casts light on the errors made at previous commits/builds.  (TODO: go into detail)


Longer Example
--------------

```scala

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

```

Data Fabric
-----------
The default storage for is:
- append-only
- idempotent
- broker-free (no central server required let N apps extend the data store without conflict)
- tracks commit and build ID on each output
- retroactively self-healing: append evidence that a commit, build, or version is untrusted to inv

(implemented, show the details)

Idempotency, Consistency, Concurrency and Testing
-------------------------------------------------
(this is implemented, and has tests, but needs a careful description)

Mapping Results, Partial Inputs and Outputs
--------------------------------------------
(also basic implementation in place, describe)

Futures and I/O
---------------
For functions that do I/O, or have other reasons to return a future, use the `WithFutureProvenance` version of the functions.  These are built to transparently let your `implFuture` return a `Future[O]`, and the system give back a `Future[Result[O]]`, instead of a `Result[Future[O]]`.

Cross-App Tracking
------------------
The result of one function with provenance might be used by another application, and the second app might not have access to all of the upstream classes that were used to make its input data. The provenance classes has a degenerate form for which the output type is "real" in scala, but the inputs are merely strings of SHA1 values of the input types.  This degenerate form is fully decomposed when we save, and only inflates into the full form when used in a library that recognizes tye types.

Deleted Code Tracking
---------------------
When a function vanishes, the same logic used for externally generated results applies.  The same SHA1 digest for the inputs is known, and the original function name, version, commit and build can be seen as strings.  But the types become inaccessible as real scala data.

Best Practices
--------------
1. Don't go too granular.  Wrap units of work in provenance tracking where they would take noticeable time to repeat, and where a small amount of I/O is worth it to circumvent repetition.
2. Be granular enough.  If the first part of your pipeline rarely changes and the second part changes often, be sure they are at wrapped in at least two different functions with provenance.  And if you go too broad _every_ change will iterate the master version.  Which defeats the purpose.
3. Pick a new function name when you change signatures dramatically.  You are safe to just iterate the version when the interface remains stable, or the new interface is a superset of the old, with automatic defaults.
4. If you want default parameters, make additional overrides to `apply()`.
5. If you want to track non-deterministic functions, make the last parameter an `Instant`, and set it to `Instant.now` on each run.  You will re-run the function, but when you happen to get identical outputs, the rest of the pipeline will complete like dominos.  This is perfect for downloaders.
6. If you use random number generation, generate the seed outside the call, and pass the seed into the call.  This lets the call become deterministic.  Even better: calculate a seed based on other inputs, if you can.  This will let you get "consistent random" results.

Project Setup:
--------------
1. Create `buildinfo.sbt` in your root by copying `buildinfo.sbt-example-{simple,multimodule}`
2. Put `addSbtPlugin("com.eed3si9n" %  "sbt-buildinfo" % "0.7.0")` into project/plugins.
3. Verify that `import YOURPACKAGE.BuildInfo works` after `sbt clean reload compile`.
4. Decide on an S3 path for your data.
5. Add one or more ResultTrackerSimple objects to your app for prod/test storage.
6. Convert key processes in your pipeline into FunctionNWithProvenance.
7. Add test cases to defend your logic.

Or, consider just copying the example1 project as a starting skeleton, and modifying to taste.

