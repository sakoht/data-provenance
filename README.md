Data Provenance
===============

This document describes the data-provenance library from the perspective of an application that uses it.

For development of the library itself, see README-DEVELOPMENT.md.

Overview
--------

The data provenance library lets an app/library wrap a functions in a way that does tracking, storage, logic versioning, and prevents duplicate work.  It forms an underlying layer of metadata upon which workflows can be defined, launched, and managed.

The developer starts with a system that contains deterministic funcitons for key processes.  By wrapping them in a `FunctionWithProvenance`, these capabilities are added:
- save output consistently
- save inputs and linking them to the outputs in the context of some function/version
- proscribe a complete workflow dependency graph and interrogating it in a separate process from which any of it is run
- examine any output and traversing the workflow chain that led to it
- track each output throroughly, including commit and build info from the software library
- "shortcut" past running the same code on the same inputs when an output has been created at the given function version
- mix deterministic, tracked data with less tracked data in clean ways
- testing in a way that can retroactively correct for logic flaws

Adding to Applications
----------------------

To start a new project quickly, copy the example1 project in this repository and modify it to taste.

For an existing project, add the following to `libraryDependencies` in the `.sbt` configuration: 
    
    `"com.cibo" %% "provenance" % "0.11.1"`

Applications that use this library must also use `SbtBuildInfo` to reveal provide commit/build information to
the provenance lib for new data generated.

Two example files are in this repository with instructions on how to add them to your project's build process:
- `buildinfo.sbt-example-simple`
- `buildinfo.sbt-example-multimodule`

To get started, you can use the `DummyBuildInfo` provided by the provenance library for use in test cases.


Background
----------

This is a review of some basics about Scala functions as a type.

A normal scala function might be declared like this:  
```scala
def foo(a: Int, b: Double): String = a.toString + "," + b.toString
```

The above takes an `Int` and a `Double`, and returns a `String` by just concatenating the others with a comma.

A longer form of the same thing in Scala uses a `Function2` object:
```scala
object foo extends Function2[Int, Double, String] {
    def apply(a: Int, b: Double): String = a.toString + "," + b.toString
}
```

Both are called the same way:
```scala
val out: String = foo(123, 9.99)
out == "123,9.99"
```

The above Scala `Function2` takes two inputs, and has three parameterized types: `I1`, `I2` and `O`.  Scala implements `Function0` - `Function22`, with inputs up to `I22`.  (A pattern followed by many of the internal classes, such as TupleN, etc.)


Adding Provenance
-----------------

To add provenance tracking, we modify the long version of a function declaration:
- `Function2WithProvenance` replaces `Function2`
- `def impl` replaces `def apply`
- `val currentVersion: Version = Version("x.x")` is added

```scala
import com.cibo.provenance._

object foo extends Function2WithProvenance[Int, Double, String] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Double): String = a.toString + "," + b.toString
}
```

The implicit contract is with the above object is:
- The `impl(..)` produces _deterministic_ results for the same inputs at any given declared version.
- The the version number will be updated when a software change intentionally change results.
- The system will track enough data that, when the above fails, the system self-corrects.


Using a FunctionWithProvenance
------------------------------

Applying the function doesn't actually _run_ the implementation.  It returns a handle, similar to a `Future`, except the
wrapped value might have been executed in the past, or in another might be in-process on another machine, or might never
be executed at all, and simply be created to interrogate and discard:

The type returned is an inner class of type `.Call`.  It is specifically constructed to capture the parameters and a `Version`:

```scala
val call1: foo.Call = foo(123, 9.99)

assert(call1.i1 == UnknownProvenance(123))
assert(call1.i2 == UnknownProvenance(9.99))
assert(call1.v == UnknownProvenance(Version("0.1")))
```

The standard way to convert a call into a result is to call `.resolve`, which will look for an existing result,
and if it isn't found, will run the implementation, make one, and save it.
```scala
val result1: foo.Result = call1.resolve     // see also resolveFuture to get back `Future[foo.Result]`
```

The result contains both the `output` and the `call` that created it, and also commit/build information for the specific
software that actually ran the `impl()`:
```scala
result1.output == "123,9.99"                // the actual output value from impl()
result1.call == ??? // == call1             // the call on the result is "deflated" but can re-vivify to the original
result1.buildInfo == YOURPACKAGE.BuildInfo  // the specific commitId and buildId of the software (see BuildInfo below)
result1.buildInfo.commitId                  // the git commit
result1.buildInfo.buildId                   // the unique buildID of the software (a high-precision timestamp).
```

Nesting
-------
A call can take raw input values, but ideally each input the result of _another_ call that produced _it_. 
This means we can track back through the history chain of data.

In addition to taking a result, it can also taka a call that _would_ produce an appropriate result. 
It will convert that into a usable result by running it, or by looking up an existing answer.

An example of nesting using a toy function:
```scala
object addMe extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int): Int = a + b
}
```

Then we could:
```scala
import io.circe.generic.auto._

val call1 = addMe(2, 3) 
val result1 = call1.resolve)

val call2 = addMe(7, 8) 

val call3 = addMe(addMe(addMe(2, 2), addMe(10, addMe(call1, result2)), addMe(5, 5)))
val result3: addMe.Result = call3.resolve
```

Note that, above, we used both raw values as inputs, and also one input is a result (`result1`) and another is a call
(`call2`).  The raw values are implicitly  converted into `UnknownProvenance[T]`, a placeholder for values with no 
history.  All such values fall under a the base type `ValueWithProvenance[T]`.  See the API Overview below for details.


Tracking Results
----------------

The `.resolve` method above, as well as and many other methods on the API, only apply in the context of an
implicit `ResultTracker`.  A `ResultTracker` is both a database for inputs, outputs, parameters, and the provenance
path that connects them.  It is also the wrapper for transitioning from call -> result (running things).

The default storage implementation is "broker-free", in that a central server or central locking is not required for
consistency, idempotency, or concurrency.  The only requirement is that whole-object writes to a given path are atomic.
It can handles concurrent attempts to do similar work "optimistically".  In a race condition identical work may be done,
but no data is corrupted or duplicated.

```scala
import com.cibo.provenance._
import io.circe.generic.auto._

implicit val bi: BuildInfo = YOURPACKAGE.BuildInfo                            // made by your app's sbt-buildinfo
implicit val rt: ResultTracker = ResultTrackerSimple("s3://mybucket/mypath") 

rt.hasResultForCall(call1) == true                                            // since the work was done above
val result1b = call1.resolve                                                  // just loads the answer made previously
result1b == result1                                                           // results match
```

There is a no-op `ResultTrackerNone` that records nothing, re-runs everything as needed.  
It can be combined with the `DummyBuildInfo` to do ad-hoc experiments, and for testing:
```scala
import com.cibo.provenance._
implicit val bi = DummyBuildInfo
implicit val rt = ResultTrackerNone()
```

DRY (Don't Repeat Yourself)
---------------------------

For a given `ResultTracker`, the same input values are never passed to the same version of the same function.
This means that when we resolve `call3` above, a result is produced for `call2`, since it is an input to `call3`. 

A subsequent invokation of `call2.resolve` will look-up the answer rather than calculate it.

A more detailed example of "shortcutting" past calculations (or "memoizing"):

```scala
import com.cibo.provenance._

object addInts extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int) = a + b
}

implicit val bi = com.cibo.provenance.DummyBuildInfo
implicit val db = ResultTrackerSimple("/my/data")

import io.circe.generic.auto._

val s1 = addInts(10, addInts(6, 6))
val r1 = s1.resolve
// ^^ calls 6+6 and then 10+12

val s2 = addInts(10, addInts(5, 7))
val r2 = s2.resolve
// ^^ calls 5+7, but skips calling 10+12 because that has been done 

val s3 = addInts(5, addInts(5, addInts(3, 4))
val r3 = s3.resolve
// ^^ calls 3+4, but skips 5+7 and 10+12, because those both have been done
```

Versions
--------

The `currentVersion` in a `FunctionWithProvenance` is how we declaratively differentiate between pure refactoring and intentional changes in results.  The implicit contract for a `FunctionWithProvenance` is that the system can trust that 
the same inputs will produce the sameoutput for the same version value.  When this is untrue, the version number should be
bumped along in the same commit (or one of the commits in the same merge) that changes the `impl`.

In practice, there will be mistakes.  A pure refactor will actually introduce a minor variation, or a component deep in the
call stack will be updated without something it uses having its version bumped.  When this happens, the data fabric can 
"self-heal". This will possibly lead to the flagging of data from certain commits as invalid, and may lead to downstream steps in a series of calls being re-run.  The intent is of the system is to make the best decision it can make at any point, without
presuming it will not later discover a flaw.

The call object stores the `version`, with a default argument that sets it to the `currentVersion`.  One might, however,
create a call with an older version specified intentionallly.  Possibly for purposes of explicitly querying for old data.

By default, only the current version will execute, with legacy versions just for bookkeeping.  If some component needs to have 
multiple versions active, however, it is possible with a small amount of code.

This versioning is intended to be more granular than the version of the whole library/application with regard to code scope,
but also to vary less frequently for a given component across git commits and release.  Most git commits that involve a
version update for some functions will not affect others.  As such, any given version of a function will likely span a range 
of git commits, and those ranges will overlap the range of other functions. (TODO: explain in a section below.  Right now 
examples are in test cases.)

There are three modes of failure when writing an `impl` and assigning/updating the `currentVersion`:
- a function that is not deterministic
- a function that that had an "impure refactor", which offers different outputs for some inputs
- a function that behaves differently for the same commit on different builds due to some external factor in the build

The system can detects the three above failure modes retroactively, as the developer "posts evidence" to a future test 
suite, which casts light on the errors made at previous commits/builds.  (TODO: go into detail)

BuildInfo
---------

An implicit `BuildInfo` (typically named) `$YOURPACKAGE.BuildInfo` implicitly available to make a `ResultTracker`.  This provides commitId, buildId, and other information to be attached to any new data made by the app.

The `BuildInfo` object used should be created by the `SbtBuildInfo` plugin.  Each project that uses the data-provenance library should use the `buildinfo.sbt` from the example repo to make this data available.

The package name used in the configuration should be in a namespace that is unique for the app/lib.  If an organization namespace is used by a wide variety of apps, make a sub-namespace for each component that is independently built and put the `BuildInfo` there, even if it is the only thing there.


Querying
--------

Previously created results can be retrieved from any FunctionWithProvenance by calling `.findResults` method.  This
returns an iterator of previous results.  There is also a `.findCalls` method that returns lighter-weight objects
that just contain metadata about the result, minus the output.

Because a software library will change over time, it is possible that values retrieved from a `ResultTracker`
were made at a time when the software structure was very different.  It is possible that the objects will no longer fully vivify.
Also, the history may cross libraries, and some classes might not be instantiatable from the application doing the query.

The `.findResults` and `.findCalls` methods will omit any results or calls that cannot be fully vivified in the current application.

Lower-level methods, `.findResultData` and `.findCallData`, always return the complete list, but in a degenerate form of
just metadata.  The `.load` method will attempt to vivify the object, possibly resulting in an exception if the class 
structure has changed, or is not available in the current application.  


Example:
```scala

case class BirthdayCake(candleCount: Int)

object BirthdayCake {
  import io.circe._
  import io.circe.generic.semiauto._
  implicit val encoder: Encoder[BirthdayCake] = deriveEncoder[BirthdayCake]
  implicit val decoder: Decoder[BirthdayCake] = deriveDecoder[BirthdayCake]
}

object addCandles extends Function2WithProvenance[BirthdayCake, Int, BirthdayCake] {
  val currentVersion: Version = Version("0.1")
  def impl(loaf: BirthdayCake, count: Int): BirthdayCake = loaf.copy(candleCount = loaf.candleCount + count)
}

val cake1 = BirthdayCake(5)
val cake2 = addCandles(cake1, 10)
val cake3 = addCandles(cake2, 6)

cake3.resolve
cake3.output shoulldBe 21

val results = addCandles.findResults
results.foreach(println)
// (BirthdayCake(15) <- addCandles(raw(BirthdayCake(5)),raw(10)))
// (BirthdayCake(21) <- addCandles((BirthdayCake(15) <- addCandles(raw(BirthdayCake(5)),raw(10))),raw(6)))
```

The application querying for results may not have the type available, possibly because it was made
in a foreign application, or because the class has changed since the data was made.

Methods that find "ResultData" instead of "Results" retrieve raw, typless data that can be interrogated
in any application.  The raw data can be converted into fully typed data with a call to the .load method,
which will succeed if the type is available, and fail otherwise.
```
val resultsRaw = addCandles.findResultData
val resutsVivified = resultsRaw.map(_.load)
assert(resultsVivified.map(_.normalize) == results.map(_.normalize)
```

Note that the `.normalize` method is only required if the code intendes to perform exact equality tests.
Without it, freshly loaded results will be lazy about loading internals until neceessary.  They are 
functionally equivalent, but will not pass a plain scala equality test.

The raw *Data access methods are available on the ResultTracker itself, so values can be found even if the
function is completely foreign:
```
val resultsRaw2 = rt.findResultData("com.cibo.provenance.addCandles")
val resultsRaw3 = rt.findResultData("com.cibo.provenance.addCandles", "0.1")
val resultsRaw4 = rt.findResultDataByOutput(Digest("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
val resultsRaw5 = rt.findResultDataByOutput(BirthDayCake(16))
```

It is possible to search for a result by one of its inputs results:
```
val result2 = eatCake(result1).resolve
val usesOfResult1 = rt.findUsesOfResult(result1)
usesOfResult1.map(_.load.normalize).head == result2.normalize
```

Or by a raw input value:
```
val usesOfOutput1 = rt.findUsesOfValue(result1.output)
usesOfOutput1.map(_.load.normalize).head == result2.normalize
```

Broad queries are also possible, revealing all data in the repository:
```
val allResults = rt.findResultData
val allCalls = rt.findCallData
```

The subset that can vivify:
```
val vivifiableResults = rt.findResults
val vivifiableCalls = rt.findCalls
```

Query more narrowly from from any `FunctionWithProvenance`:
```
val all: Iterable[eatCake.Result] = eatCake.findResults
val all: Iterable[eatCake.Call] = eatCake.findCalls

val all: Iterable[FunctionCallResultWithProvenanceSerializable] = eatCake.findResultData
val all: Iterable[FunctionCallWithProvenanceSerializable] = eatCake.findCallData
```

Drill down:
```
val names: Iterable[String] = rt.findFunctionNames
val versions: Iterable[Version] = rt.findFunctionVersions("com.cibo.provenance.eatCake")
val results = rt.findResultData("com.cibo.provenance.eatCake", Version("0.1))
val calls = rt.findCallData(("com.cibo.provenance.eatCake", Version("0.1))
```

Tags
----
Any result can be "tagged" with a text String annotation.  

Tags are similar to tags in source control, except the same tag name can be applied to multiple things.

Given some result:
```
val result1 = eatCake(...).resolve
```

Tag it:
```
result1.addTag("tag 1")
```

Find the result later by tag:
```
val result1b = eatCake.findResultsByTag("tag 1").head 
result1b shouldEqual result1
```

Query the ResultTracker for the data in raw/generic form, accessable across libraries:
```
val result1c = rt.findResultDataByTag("tag 1").head
result1c.load  shouldEqual result1
```

Find tags by the data type of the result they reference:
```
val tag1b = rt.findTagsByOutputClassName("com.cibo.provenance.BirthdayCake").head
tag1b shouldEqual Tag("tag 1")
```

Or by the type of function call result they apply to:
```
val tag1c = rt.findTagsByResultFunctionName("com.cibo.provenance.eatCake").head
tag1c shouldEqual Tag("tag 1")
```

Find it in the list of all tags in the result tracker:
```
rt.findTags.contains(Tag("tag 1")
```

Remove a tag:
```
result1.removeTag("tag 1")
```

Find all history, including each additions and removals in the append-only repository:
```
val history = rt.findTagHistory.sortBy(_.ts)
history.size shouldEqual 2

val add = history.head
val remove = history.last

add.addOrRemove shouldEqual AddOrRemoveTag.AddTag
add.subject.load.mormalize shouildEqual result1.normalize
add.tag shouldEqual Tag("tag 1")

remove.addOrRemove shouldEqual AddOrRemoveTag.RemoveTag
remove.subject.load.mormalize shouildEqual result1.normalize
remove.tag shouldEqual Tag("tag 1")

add.ts < remove.ts
```

Since old versions of results might not fully vivify when the class changes,
or if the class is not available to the querying library,
it is possible to query for the raw metadata w/o fully vivifying the results.

Calls described as "ResultData" intead of "Result" return untyped metadata
accessible in ay application.
```
val callsAsRawData = eatCake.findResultDataByTag("tag 1")
```

The untyped metadata can be loaded in any application where the type is available, presuming the type
has not changed dramatically enough that it cannot deserialize: 
```
val justTheOnesWeCanVivify: Iterable[eatCake.Result] =
    callsAsRawData.flatMap {
        primitiveData >
            Try(primitiveData.load) match {
                case Success(realObject) => Some(realObject.asInstanceOf[eatCake.Result])
                case Failure(err) => None
            }
    }
```

When a result is tagged, it is saved immediately.

A call can also be tagged, but it the tag addition must be resolved in order to save the tag.
```
val call1 = eatCake(...)
val tag1 = call1.addTag(Tag("tag 2")

call1.resolve
// tag1 is not saved yet
tag1.resolve
// tag1 is now saved
```

NOTE: The same tag text can be applied to different result objects independently.
All results attached to the same tag can be loaded as a group.  Removing a tag from one
result does not remove it from all results. 


Short Example
-------------

```scala
import com.cibo.provenance._


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
  implicit val rt: ResultTracker = ResultTrackerSimple(args(0))   //"s3://mybucket/myroot")

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
```

Mapping Results, Partial Inputs and Outputs
--------------------------------------------

When a `FunctionWithProvenance` returns a Seq-like thing, the Call and Result have additional methods that operate on the underlying output with provenance tracking:
- `.map`
- `.indices`
- `.apply`

This means you can have: f1 produce a list, f2 and f3 operate on members individually, and f4 process the final list, like this:
```
f4(f1().map(f2).map(f3))    
```

This records things at two levels:
- a single four-step path of the top-level logic: `f4 <- MapWithProvenance(f3) <- MapWithProvenance(f2) <- f1()`
- a parallel path for _each_ member of the `f1` list: `f4 <- gather(n) <- f3 <- f2 <- apply(n) <- scatter < f1()`

Note that only the first layer exists when nothing has resolved.  Upon resolution, the granular path resolves first,
including the gather at the end, then the call to f4.  The actual intermediate lists are logically present but never
materialized unless explicitly requested.

A similar wrapping happens around `FunctionWithProvenance[Option[_]]`, allowin an output option and provenance tracking around the creation or extraction.

Longer Example
--------------
```scala

import com.cibo.provenance._

object addMe extends Function2WithProvenance[Int, Int, Int] {
  val currentVersion = Version("0.1")
  def impl(a: Int, b: Int): Int = a + b
}

object myApp2 extends App {

  implicit val bi: BuildInfo = DummyBuildInfo                       // use com.mycompany.myapp.BuildInfo
  implicit val rt: ResultTracker = ResultTrackerSimple("s3://...")

  import io.circe.generic.auto._

  // Basic use: separate objects to represent the logical call and the result and the actual output.
  val call1 = addMe(2, 3)             // no work is done
  rt.hasResultForCall(call1)          // false (unless someone else did this)

  val result1 = call1.resolve         // generate a result, if it does not already exist
  println(result1.output)             // get the output: 5
  println(result1.call)         // get the provenance: call1
  rt.hasResultForCall(call1)          // true (now saved)

  val result1b = call1.resolve        // finds the previous result, doesn't run anything
  result1b == result1

  // Nest:
  val call2 = addMe(2, addMe(1, 2))
  val result2 = call2.resolve                       // adds 1+2, but is lazy about adding 2+3 since it already did that
  result2.output == result1.output                  // same output
  result2.call != result1.call          // different provenance

  // Compose arbitrarily:
  val bigPlan = addMe(addMe(6, addMe(result2, call1)), addMe(result1, 10))

  // Don't repeat any call with the same inputs even with different provenance:
  val call3: addMe.Call = addMe(1, 1)               // (? <- addMe(raw(1), raw(1)))
  val call4: addMe.Call = addMe(2, 1)               // (? <- addMe(raw(2), raw(1)))
  val call5: addMe.Call = addMe(call3, call4)       // (? <- addMe(addMe(raw(1), raw(1)), addMe(raw(2), raw(1))))
  val result5 = call5.resolve                       // runs 1+1, then 2+1, but shortcuts past running 2+3 because we r1 was saved above.
  assert(result5.output == result1.output)          // same answer
  assert(result5.call != result1.call)  // different provenance

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
```


Data Fabric
-----------
The storage system uses serialization and digests to capture the inputs and outputs of each call,
along with detail on the specific commit and build for each output.

The native storage system is:
- append-only
- idempotent
- optimistically concurrent (but you can add locking on top if you choose)
- broker-free (no central server required let N apps extend the data store without conflict)
- retroactively self-healing (append retroactive evidence that a prior commit, build, or version is untrusted)

At a lower-level, it is: 
- "physically" fault-tolerant without transactions
- mergeable
- subdividable

### Example Storage Tree:
- source: `test/resources/expected-output/scala-2.12/rerun1.manifest`
- generated by the test suite resolving `com.cibo.provenance.Add(1, 2)` 
```
data/5ba88e7374faa02b379d45f0c37ba3420d8742c6
data/83e06c74c77009e510e73d8c53d64be2e1600a71
data/cbf6eb1142cf44792e76a86e0d32fd89f94935a9
data-provenance/5ba88e7374faa02b379d45f0c37ba3420d8742c6/as/Int/from/-
data-provenance/83e06c74c77009e510e73d8c53d64be2e1600a71/as/Int/from/-
data-provenance/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/as/Int/from/com.cibo.provenance.Add/1.0/with-inputs/805b0523984a5b175938cfbdcd04015d6ee41ec4/with-provenance/54b994cc3625bd54213e4dc9017b98c85cabedff/at/DUMMY-COMMIT/1955.11.12T22.04.00Z
functions/com.cibo.provenance.Add/1.0/input-group-values/805b0523984a5b175938cfbdcd04015d6ee41ec4
functions/com.cibo.provenance.Add/1.0/inputs-to-output/805b0523984a5b175938cfbdcd04015d6ee41ec4/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/DUMMY-COMMIT/1955.11.12T22.04.00Z
functions/com.cibo.provenance.Add/1.0/output-to-provenance/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/805b0523984a5b175938cfbdcd04015d6ee41ec4/54b994cc3625bd54213e4dc9017b98c85cabedff
functions/com.cibo.provenance.Add/1.0/call-inputs/54b994cc3625bd54213e4dc9017b98c85cabedff/805b0523984a5b175938cfbdcd04015d6ee41ec4
functions/com.cibo.provenance.Add/1.0/calls/690bd39b1116c99020ee054e2b7db238a76f0d19
commits/DUMMY-COMMIT/builds/1955.11.12T22.04.00Z/9b95f61961d21c6e90064e7146b5d06e6c936e25
```

### Breakdown:

Each time a FunctionCallResultWithProvenance saves, a set of paths go into the data store.  Each path is, also,
and independent assertion of truth, and might overlap paths form previous saves.  As such, a "partial save" is not
problematic.  A process that terminates mid-save does not leave data that requires cleanup, as long as individual,
per-path, saves are atomic.

The raw filesystem implementation makes writes atomic by writing to a path with an extension, and then doing a "rename"
to the final path.  Nearly all modern filesystems support atomic rename, including NFS. 

All paths fall into two categories:
- a path that ends in a digest suffix, for which the digest is the SHA1 of the bytes of the content 
- a path with empty content, for which the the "information" is the path key itself

#### `data/`:

The master data tree of all input and output values lives under `data/`.  In this example, these are the simple 
serializations of the integers 1, 2 and 3, respectively.  The content is a serialzation of each into a `Array[Byte]`,
then a SHA1 digest of those bytes become the ID:
```
data/5ba88e7374faa02b379d45f0c37ba3420d8742c6
data/83e06c74c77009e510e73d8c53d64be2e1600a71
data/cbf6eb1142cf44792e76a86e0d32fd89f94935a9
```

#### `data-provenance/`
```
data-provenance/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/as/Int/from/com.cibo.provenance.Add/1.0/with-inputs/805b0523984a5b175938cfbdcd04015d6ee41ec4/with-provenance/54b994cc3625bd54213e4dc9017b98c85cabedff/at/DUMMY-COMMIT/1955.11.12T22.04.00Z
data-provenance/5ba88e7374faa02b379d45f0c37ba3420d8742c6/as/Int/from/-
data-provenance/83e06c74c77009e510e73d8c53d64be2e1600a71/as/Int/from/-
```

This is the master index into the data.  It is a primary link between any output and all of the sources that produce it.

The first example asserts that the value `3` came from calling `com.cibo.provenance.Add` at version `1.0` with
 parameters `1` and `2`, and was resolved at the specified commit and build.  

The second and third examples show a `-` instead of call details, signify that the integers `1` & `2`, respectively,
were used with unknown provenance.  


#### `commits/`
```
commits/DUMMY-COMMIT/builds/1955.11.12T22.04.00Z/9b95f61961d21c6e90064e7146b5d06e6c936e25
```
When a result is saved, the BuildInfo for the specific commit and build is saved here, exactly once per build.


#### `functions/`:
```
functions/com.cibo.provenance.Add/1.0/
```
Details about all calls live under the `functions/` tree, with sub-trees by function name and version.

This tree never contains any real serialized inputs or outputs, just linkage between IDs that are under `data/`. 

Sub-keys are described below:

##### `inputs-group-values/`:
```
functions/com.cibo.provenance.Add/1.0/input-group-values/805b0523984a5b175938cfbdcd04015d6ee41ec4
```

The input-group-values path captures the assertion that the inputs `(1, 2)` are used as an input.  It is literally
the serialization of the Seq of inputs after converting them to digest strings.  This path has content, but is very
small:

##### `inputs-to-output/`:
```
functions/com.cibo.provenance.Add/1.0/inputs-to-output/805b0523984a5b175938cfbdcd04015d6ee41ec4/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/DUMMY-COMMIT/1955.11.12T22.04.00Z
```

The inputs-to-output path is a zero-size file is the central assertion that specific inputs yield a given output.  It is
the last path saved, and is the path that allows subsequent functions to "shortcut" past an actual run.

Note that the above captures the exact commit and build at which the implementation was called to produce the output.
In the ideal/correct case, one group of inputs at a given version will produce one output, and all further attempts to 
resolve that function at those parameters will re-use that output.  

See "Races, Conflicts and Collisions" below for details on situations which result in multiple "children per parent"
in this path structure, what they signify, and how they are handled.


##### `calls/`
```
functions/com.cibo.provenance.Add/1.0/calls/0f2bbc4919dd3e9272fb8f5b77058940d0b658eb
```
The `calls/` path captures all `FunctionCallWithProvenanceSerialized` including its input `FunctionCallResultWithProvenance`s.

##### `output-to-provenance/`:
```
functions/com.cibo.provenance.Add/1.0/output-to-provenance/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/805b0523984a5b175938cfbdcd04015d6ee41ec4/54b994cc3625bd54213e4dc9017b98c85cabedff
```
This captures the FunctionCallResultWithProvenance to a given output/input pair.  This tree _will_ often have multiple
child paths under any parent path, when multiple inputs happen to produce the same outupt, or when multiple upstream
paths lead to the same inputs. 

##### `inputs-to-outputs/`:
```
functions/com.cibo.provenance.Add/1.0/call-inputs/54b994cc3625bd54213e4dc9017b98c85cabedff/805b0523984a5b175938cfbdcd04015d6ee41ec4
```
This "completes the loop", allowing a given provenance object to look up its inputs.  The inputs can hypothetically be
known before the implementation executes, but they are typically only saved when other data is saved.


API Part 1
-----------

### Class Hierarchy Example:

Given:
```scala
import com.cibo.provenance._

object foo extends Function2WithProvenance[Int, Double, String] {
    val currentVersion = Version("0.1")
    def impl(i: Int, d: Double): String = ???  
}
```

The function singleton `foo` has the following class hierarchy:
```
foo
  <: Function2WithProvenance[Int, Double, String]
    <: FunctionWithProvenance[String]
```

When `foo(i, d)` is invoked, a `foo.Call` is returned:
```
foo.Call
  <: Function2CallWithProvenance[Int, Double, String]
    <: FunctionCallWithProvenance[String]
      <: ValueWithProvenance[String]
```

When `.resolve` is invoked on the `foo.Call`, a `foo.Result` is returned:
```
foo.Result
  <: Function2CallResultWithProvenance[Int, Double, String]
    <: FunctionCallResultWithProvenance[String]
      <: ValueWithProvenance[String]
```

Note that, to resolve, a `ResultTracker` must be provided implicitly to make a `.Result`, but
a `.Call` is independent of storage/tracking.


### Common Usage:

There are six classes that applications must interact-with directly:
- `Function{n}WithProvenance`: the base classes for components in the app that get tracking
- `.Call` and `.Result`: inner-classes for each function declared.
- `Version`: a simple wrapper around version numbers/names that are declared in each component
- `ResultTracker`: the base class for storage
- `BuildInfo`: the base class for the singleton that holds build information for the app

A minimal application will:
- define a bunch of `object myFunction extends Function{n}WithProvenance[...]` to do work with tracking
- give each component a `val currentVersion = Version("0.1")`
- define a `YOURPACKAGE.BuildInfo` in buildinfo.sbt
- have a `implicit val rt = ResultTrackerSimple("s3://mybucket/myroot")(YOURPACKAGE.BuildInfo)`
    above any code that actually resolves calls, checks for results, or otherwise interacts with storage

Note: all inputs and outputs need to have a circe encoder/decoder available when used.
The easiest solution is to have `import io.circe.generic.auto._` above code that makes calls or resolves them.
This is not needed if every input and output declares an implicit encoder/decoder in its companion object.

### The Core Function*WithProvenance Classes

The three core classes in the API are:
- `FunctionWithProvenance[O]`           A singleton object representing a block of logic and a current version ID.
- `FunctionCallWithProvenance[O]`       A specific set of params for the function, plus a version ID for the function.
- `FunctionCallResultWithProvenance[O]` A the output value of a call, plus the commit and build IDs responsible.

Note that they are all typed for the output type of the function in question, represented as `O` at this level.

Together, these deconstruct what happens normally with a function call, but which goes untracked in the common case:
- The "call" captures all information logically required to generate the result (parameters + function version).
- The "result" references the call, and captures the actual output, and specifics of the git commit and software build.

### Arity-Specific Subclasses

The core three classes each get expanded into 22 subclasses, each with specific arity from 0 to 21, with full
type signatures:
- `Function{0..21}WithProvenance[I1, I2, ..., O]`
- `Function{0..21}CallWithProvenance[I1, I2, ..., O]`
- `Function{0..21}CallResultWithProvenance[I1, I2, ..., O]`

### Final Specific Classes

Applications subclass `Function{n}WithProvenance` directly when writing new component objects.  Those
component objects embed a function-specific `.Call` and `.Result`:

In the example above, the `foo` function declaration creates two classes:
- a `foo.Call` inner class that extends `Function2CallWithProvenance[Int, Double, String]`
- a `foo.Result` inner class that extends `Function2CallResultWithProvenance[Int, Double, String]`

The two are mutually paired.  A `foo.Call` knows it makes a `foo.Result`.  A `foo.Result` comes from a `foo.Call`.

The signatures are fully-qualified:
```scala
val call: foo.Call = foo(123, 9.99)
val result: foo.Result = call.resolve
```

### The Base Trait ValueWithProvenance & Call Parameters

Both "calls" and "results" are subclasses of the sealed trait `ValueWithProvenance[O]`.  This base type is
a used for the _inputs_ of new calls.  As such, a function that logically takes an `Int` input can actually 
construct a with either a `FunctionCallResultWithProvenance[Int]` or a `FunctionCallWithProvenance[Int]`.

A call with an input type `T` can also take a `T` directly.  When a raw value is used that is not a 
`ValueWithProvenance[T]`, there is an implicit converter that creates an `UnknownProvenance[T]`.  This is a special 
case of `Function0CallWithProvanence[T]` that take zero parameters and returns a constant value used to bootstrap
data tracking.  

The `UnknownProvenance[T]` also has a companion `UnknownProvenanceValue[T]`, which is a special case of 
`Function0CallResultWithProvenance[T]`.

The common methods to go between calls and results are:
- `.resolve`:      Turns a call into a result, possibly by running the logic.  (On a result, just returns itself.)
- `.unresolve`:    For a result, returns its call.  (On a call, just returns itself.)

Since the inputs to a call are all `ValueWithProvenance[I{n}]`, it is possible for a "call" to represent the complete
workflow that led/leads to a particular input value, and to represent it at any state of resolution.  

Terminology Note:
- A `FunctionCallWithProvenance` is said to have provenance _before_ it executes
  because it knows where its _inputs_ came-from/will-come-from.
- A `FunctionCallResultWithProvenance` has provenance because it knows knows both originating call,
  plus the `BuildInfo` that produced the output, and the output value itself.
- The iterative resolution process replaces calls in the tree with results.

API Part 2
-----------

### VirtualValue[T]

A `FunctionCallResultWithProvenance` references its output with a `VirtualValue[O]`.

This has 3 Options, at least one of which must not be None:
- the actual value of type T
- the serialized bytes of type T (which can be decoded into a T if the class is present)
- the digest of the serialized bytes of T (which can be used to retrieve the bytes from a result tracker)

When a result is saved, and its output serialized, the output can effectively be reduced to its database ID,
shrinking the memory footprint.

By default the result `.outputAsVirtualValue` holds the actual value `T` and the `Digest` after being saved.  When
re-inflated (described below) the actual data re-vivifies lazily. 

### Internal Classes:

Since a `FunctionCallWithProvenance` can have nested inputs of arbitrary depth, the size of a call tree
can possibly get extremely long, with each new call adding a layer.  Saving each naively would mean that every object
saves out an ever-growing history object, most of which is copied in ints predecessor.

These classes are not mostly invisible to regular usage, but are still accessible through the public API.  They
are used to represent data as it transitions to storage, and as it is passed along a wire protocol.

#### `*Serializable`

`*Serializable` classes are serializable equivalents to the `ValueWithProvenance` hierarchy.  They:
- represent type information as strings instead of real types
- are broken into granular pieces
- are the objects that are _actually_ converted into JSON and saved

When a complicated provenance history is saved, each addition effectively appends a few of these into the tracking system for each new call.

#### `*Saved`

These are wrappers for the `*Serializable` classes that are aware of _output_ type, and are valid members of the regular `ValueWithProvenance` hierarchy.  When saving, the original objects are replaced with these.  When re-loading objects they come back in this form, and only fully vivify historical inputs when `.load` or `.loadRecurse` is called. 


#### Deflation & Inflation

When a result it saved, it is converted into a `FunctionCallResultWithProvenanceSerializable`, as are its 
inputs, recursively.  These are, in turn, composed of a `FunctionCallWithProvenanceSerializable`, an output value _digest_, 
and a light version of the `BuildInfo` that contains only the commit and build ID values.  

These reference each other by digest ID rather than by software reference, but since serialization is consistent, 
the same ID will have the same history in any tracking system that stores it.

The serialized data endpionts are returned wrapped in `FunctionCall{Result}WithProvenanceSaved[O]`, which act as drop-in replacements for the fully-vivified original.  These are part of the `ValueWithProvenance[_]` sealed trait, so any call can contain calls/results that are deflated after some depth.

The methods to take a call or result back and forth from its deflated and inflated states are:
- `.save`: returns the *Deflated equivalent of an inflated object after recursively saving everything
- `.load`: returns the inflated equivalent of a deflated object, or itself if already inflated

Extening history with a single call actually just appends the following to storage:
1. the serialized value of the output
2. the serialized value of any inputs that were not already saved (usually only when values with UnknownProvenance are supplied)
3. a single small `FunctionCallWithProvenanceSerialized` object which:
 - embeds a serialized version of each of its `FunctionCallResultWithProvenanceSerialized`
4. the list of IDs of the output values of all if the input functions
5. the new associations between the above represented by the new result

The `FunctionCallResultWithProvenanceSerialized` embeded contains the ID used in the inputs contains the IDs of the underlying calls, not the full recursive input chain, so it appends naturally without bloat.  A single digest ID can represent the entire history.  When histories intersect they data there is no duplication.

It is possible that a new result will simply include #5 above, if, for instance, the same function was called previously on the with the same inputs, with those inputs coming from different source functions, all of which were, themselves, called at other times in some other context.  Or is possibly a no-op if there is a race condition that causes the result to be generated twice in parallel.


#### Inconsistent Operations

Each FunctionWithProvenance is expected to produce consistent output for the same inputs at the same function version.

In some situations, a process is known to possibly produce different data over time.  There are two options:
1. Don't use provenance tracking with this function, just use it with the result.
2. Add an optional timestamp parameter, and set it to Instant.now.

In the latter case, when the function returns the same data as a prior execution, a tiny record is made with the timestamp,
and subsequent use of the result "shortcuts", since the output value has been seen before.

The only down-side of option two is that, if the code attempts to run often, but produces the same answer,
you may fill your storage with tiny records indicating each time the attempt was made. 

Idempotency and Concurrency
---------------------------

Because the storage is structured to be append-only without a broker, all partial operations leave storage in a valid
state.  As such the data management layer processes are idempotent.

A large number of concurrent operations can perform similar work without colliding.  The down-side of overlapping work
is the potential waste of compute time and network bandwidth, not data corruption or conflicts.

By avoiding any locking mechanisms, the system is horizontally scalable.  To shift from optimistic to pessimistic 
concurrency, override to the ResultTracker to lock on the hash key of the inputs, plus the function and version, 
during resolution.  This will prevent some wasted resources in exchange for possible contention.  This is not
currently a configured option in the default trackers.

Automated Testing
-----------------

#### Setup

The `ResultTrackerForTest` helps create test cases in the application/library that uses tracking.

It is a two-layer tracker where the reference data (expected results) are in the bottom layer,
and new data made during the test run, if any, sits in the top layer.  


Outside the test case, one time in the repo, make a factory for test trackers:
```    
    object MyTestTrackerFactory extends ResultTrackerForTestFactory(
      outputRoot = SyncablePath(s"/tmp/${sys.env.getOrElse("USER","anonymous")}/result-trackers-for-test-cases/myapp"),
      referenceRoot = SyncablePath(
        new File("libprov/src/test/resources/provenance-data-by-test").getAbsolutePath
      )
    )(com.mycompany.myapp.BuildInfo)
```
(For integration tests, swap the 2nd parameter for an s3 path.)


This example function will be used below:
```    
    object MyTool1 extends Function2WithProvenance[Int, Int, Int] {
      val currentVersion = Version("0.1")
      def impl(a: Int, b: Int): Int = a + b
    }
```

This test case tests the function with 3 scenarios:
```    
    implicit val bi: BuildInfo = BuildInfoDummy
    implicit val rt: ResultTrackerForTest = MyTestTrackerFactory("test-tracker-for-mytool1")
    
    it("Test MyTool with various parameters.") {
      rt.clean()
      MyTool1(2, 3).resolve
      MyTool1(6, 4).resolve
      MyTool1(7, 7).resolve
      rt.check()
    }
```

The first time this runs, there is no reference data.  Everything will pass until the the check() method,
at which point an `ResultTrackerForTest.UnstagedReferenceDataException` will be thrown.

The error message tell you what to do to stage the data:
```
# New reference data! To stage it:
rsync -av --progress /tmp/$USER/result-trackers-for-test-cases/libprov/test-the-tester/ $HOME/myapp/src/test/resources/provenance-data-by-test/test-the-tester
```

After staging the data as directed, re-run and it should pass.


#### Catching Errors

If code ever produces an inconsistent answer for the same params & version, the ResultTrackerForTest throws an exception.

To see this, change the function impl(), but do not update the currentVersion:
```
    object MyTool1 extends Function2WithProvenance[Int, Int, Int] {
      val currentVersion = Version("0.1")
      def impl(a: Int, b: Int): Int = a + b + 999
    }
```

The suite will now throw a `com.cibo.provenance.exceptions.InconsistentVersionException`.

Typically at this point you would recognize that adding 999 was a bad idea and revert the change.


#### Upping the Version for Intentional Changes

If the new result in the above example were NOT an error, you would increment the currentVersion, say to `Version("0.2")`.

The next test run would not fail to resolve, since the new version keeps the results from conflicting with the old.

The check() call will complain on the first run that there is new reference data, though, since the v0.2 results will
not exist in the test suite.  

Follow the instrucitons in the error message to stage the data, and the test should be passable.

Commit the new reference data along with the updated tool logic.
   

Futures and I/O
---------------
For functions that perform I/O, or return Futures, it is best to have the inner portion of the function be 
broken out and have provenance tracking.  The outer function can take futures, and map them over the inner
function that runs on tangible values and does the tracking.


Cross-App Tracking
------------------
The result of one function with provenance might be used by another application, and the second app might not have access to all of the upstream classes that were used to make its input data. The provenance classes has a degenerate form for which the output type is "real" in scala, but the inputs are merely strings of SHA1 values of the input types.  This degenerate form is fully decomposed when we save, and only inflates into the full form when used in a library that recognizes tye types.

Deleted Code Tracking
---------------------
When a function is deleted from the code base, the same logic used for externally generated results applies.  The same SHA1 digest for the inputs is known, and the original function name, version, commit and build can be seen as strings.  But the types become inaccessible as real scala data.

Best Practices
--------------
1. Don't go too granular.  Wrap units of work in provenance tracking where they would take noticeable time to repeat, and where a small amount of I/O is worth it to circumvent repetition, record status, etc.  An initial pipeline often has just one step or two to four steps.  Add tracking granularity where it really helps.
2. Be granular enough.  If the first part of your pipeline rarely changes and the second part changes often, be sure they are at wrapped in at least two different functions with provenance.  And if you go too broad _every_ change will iterate the master version.  Which defeats the purpose.
3. Pick a new function name when you change signatures dramatically.  But ou are safe to just iterate the version when the interface remains stable, or the new interface is a superset of the old, with automatic defaults.
4. If you want default parameters, make additional overrides to `apply()`.
5. If you want to track non-deterministic functions, make the last parameter an `Instant`, and set it to `Instant.now` on each run.  You will re-run the function, but when you happen to get identical outputs, the rest of the pipeline will complete like dominos.  This is perfect for downloaders.
6. If you use random number generation, either generate the seed outside the call, and pass the seed into the call.  This lets the call become deterministic.  Even better: calculate a seed based on other inputs, if you can.  This will let you get "consistent random" results.

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


Wrapping Object Construction
----------------------------
It is common for key functional logic to exist behind object construction.

To avoid writing a custom function every time you want to wrap an object with tracking,
use the `ObjectCompanion{0..20}` subclasses for the _companion_ class to any object you want to track.

It adds the `.withProvenance` special constructor, which wraps the underlying constructor with tracking.

Example:
```
import com.cibo.provenance._
import com.cibo.provenance.oo._

case class MyClass(foo: Int, bar: String)

object MyClass extends ObjectCompanion2[Int, String, MyClass] {
  implicit val encoder = deriveEncoder[MyClass]
  implicit val decoder = deriveDecoder[MyClass]
}
```

This now works:
```
val obj = MyClass.withProvenance(myFoo, myBar)
```


Wrapping Property Access
------------------------
Often, one step in a process returns a `Product` (case class), and only specific elements 
need to be passed to some other `FunctionWithProvenance`.

To avoid writing a bunch of tiny custom `FunctionWithProvenance` subclasses for each case,
there is a type class, `ProductWithProvenance`.  It makes it easy to expose the case class properties
directly on the call/result, much like we do with map() and apply() on `Traversables` 

An example:
```
import com.cibo.provenance._
import com.cibo.provenance.oo._

case class Fizz(a: Int, b: Double)

object Fizz extends ObjectCompanion2[Int, Double, Fizz](NoVersion) { outer =>
  implicit def encoder: Encoder[Fizz] = deriveEncoder[Fizz]
  implicit def decoder: Decoder[Fizz] = deriveDecoder[Fizz]

  implicit class FizzWithProvenance(obj: ValueWithProvenance[Fizz]) extends ProductWithProvenance[Fizz](obj) {
    val a = productElement[Int]("a")
    val b = productElement[Double]("b")
  }
}

val obj = Fizz.withProvenance(123, 9.87)
obj.a.resolve.output shouldBe 123
obj.b.resolve.output shouldBe 9.87
```
   
Wrapping Method Calls
---------------------

Once the object construction is wrapped in tracking, you often have methods calls,
rather than classical functions, that need tracking.  The `ObjectCompanion` class 
adds `mkMethod{0..20}` to make doing this minimally invasive.


For this example class:
```
case class Boo(i: Int) {
  def incrementMe: Int = i + 1

  def addToMe(n: Int): Int = i + n

  def catString(n: Int, s: String): String =
    (0 until (i + n)).map(_ => s).mkString("")
}
```

We have  the following companion object wrapping each method:
```
object Boo extends ObjectCompanion1[Int, Boo](Version("0.1")) { self =>
  implicit val encoder: Encoder[Boo] = deriveEncoder[Boo]
  implicit val decoder: Decoder[Boo] = deriveDecoder[Boo]

  // Add tracking for methods on Boo that we intend to call with tracking.
  val incrementMe = mkMethod0[Int]("incrementMe", Version("0.1"))
  val addToMe = mkMethod1[Int, Int]("addToMe", Version("0.1"))
  val catString = mkMethod2[Int, String, String]("catString", Version("0.1"))

  // Make a type class so these can be called in a syntactically-friendly way.
  implicit class WrappedMethods(obj: ValueWithProvenance[Boo]) {
    val incrementMe = self.incrementMe.wrap(obj)
    val addToMe = self.addToMe.wrap(obj)
    val catString = self.catString.wrap(obj)
  }
}
```

Usage is like this:
```
val obj = Boo.withProvenance(2)
obj.incrementMe().resolve.output shouldBe 3
obj.addToMe(90).resolve.output shouldBe 92
obj.catString(3, "z").resolve.output shouldBe "zzzzz"
```
