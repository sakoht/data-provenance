Data Provenance
===============

This document describes the data-provenance library from the perspective of an application that uses it.

For development of the library itself, see README-DEVELOPMENT.md.


Adding to Applications
----------------------
An applications should add the following to its `libraryDependencies`: 
    
    `"com.cibo" %% "provenance" % "0.2"`

Applications that use this library must also use `SbtBuildInfo` to reveal provide commit/build information to
the provenance lib for new data generated.

Two example files are in this repository with instructions on how to add them to your project's build process:
- `buildinfo.sbt-example-simple`
- `buildinfo.sbt-example-multimodule`

To get started, you can use the `DummyBuildInfo` provided by the provenance library for use in test cases.


Background
----------

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

The above Scala `Function2` takes two inputs, and has three parameterized types: the output type followed by each of 
the input types. Scala implements `Function0` - `Function22`.  This pattern of having `SomeModadN[T]` is similar for 
many builtin classes in the scala core, and in many libraries.This pattern is similar for many builtin classes.
See classes like `Tuple1`-`Tuple22` for common examples.


Adding Provenance
-----------------

To add data-provenance we modify the long version of a function declaration:
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
- the `impl(..)` produces deterministic results for the same inputs at any given declared version
- the the version number will be updated when a software change intentionally change results
- the system will track enough data that, when the above fails, the system self-corrects


Using a FunctionWithProvenance
------------------------------

Applying the function doesn't actually _run_ the implementation.  It returns a handle, similar to a `Future`, except the
wrapped value might have been executed in the past, or in another might be in-process on another machine, or might never
be executed at all:

The type returned is an inner class of type `.Call`, specifically build to capture the parameters an a version:
```scala
import io.circe.generic.auto._

val call1: foo.Call = foo(123, 9.99)
call1.i1 == UnknownProvenance(123)
call1.i2 == UnknownProvenance(9.99)
call1.v == UnknownProvenance(Version("0.1"))
```

The call might later be executed, or passed to another process, queued, or used as a parameter for a database query.

The standard way to convert a call into a result is to call `.resolve`, which will look for an existing result,
and if it isn't found, will run the implementation, make one, and save it.
```scala
val result1: foo.Result = call1.resolve     // see also resolveFuture to get back `Future[foo.Result]`
```

The result contains both the `output` and the `call` that created it, and also commit/build information for the specific
software that actually ran the `impl()`:
```scala
result1.output == "123,9.99"                // the actual output of impl()
result1.call == call1.deflate               // the logical call that made it (in a "deflated" form, see Inflation and Deflation below)
result1.buildInfo == YOURPACKAGE.BuildInfo  // the specific commitId and buildId of the software
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

Versions and BuildInfo
----------------------

The version in the function is an "asserted version".  A declaration by the programmer that outputs will be consistent 
for the same inputs.

The call specifies the `version`, with a default argument that sets it to the `currentVersion`.  One might create a call
with an older version for purposes of explicitly querying for old data.  The receipient of a call might inspect the
version of a call.

When software is behaving as intended, the version is sufficient to describe a single iteration of function logic.  
There will be multiple repository commits, and multiple source code builds, typically, that have the same version 
number for a component, since other components will be iterating in parallel.

In theory, the versions will be updated appropriately.  In practice, errors will occur.

There are three modes of failure:
- a function that does not really produce the same output for the same inputs repeatably at any commit
- a function that is refactored at some commit, but a change in results is introduced inadvertently
- a function that behaves differently for the same commit on differnt builds, due to some external factor in the build 
process

Each actual output produced tracks the exact git commit and build ID used to produce it.  This comes from SbtBuildInfo.  
Each project that uses the data-provenance library should use the `buildinfo.sbt` from the example repo to make this 
data available.  The BuildInfo created must be implicitly available to make a ResultTracker.

The system can detects the three above failure modes retroactively, as the developer "posts evidence" to a future test 
suite, which casts light on the errors made at previous commits/builds.  (TODO: go into detail)

Shorter Example
---------------
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
  implicit val bi: BuildInfo = com.cibo.provenance.DummyBuildInfo // use com.mycompany.myapp.BuildInfo
  implicit val rt: ResultTracker = ResultTrackerSimple("s3://...")

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
- `Function{0..21}CallWithProvenance[I1, I2, ..., O]`

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

### Deflation & Inflation

Since a `FunctionCallWithProvenance` can have nested inputs of arbitrary depth, the size of a call tree
can possibly get extremely long, with each new call adding a layer.  Saving each naively would mean that every object
saves out an ever-growing history object, most of which is copied in ints predecessor.

To handle this, when a result it saved, it is converted into a `FunctionCallResultWithProvenanceDeflated`, as are its 
inputs, recursively.  These are, in turn, composed of a `FunctionCallWithProvenanceDeflated`, an output value _digest_, 
and a light version of the BuildInfo that contains only the commit and build ID strings.  These reference each other
by digest ID rather than by software reference, but since serialization is consistent, the same ID will have the 
same history in any tracking system that stores it.

These are part of the `ValueWithProvenance[_]` sealed trait, so any call can contain calls/results that are deflated
after some depth.

The methods to take a call or result back and forth from its deflated and inflated states are:
- `.deflate`: returns the *Deflated equivalent of an inflated object, or itself if already deflated
- `.inflate`: returns the inflated equivalent of a deflated object, or itself if already inflated

Extening history with a single call actually just appends the following to storage:
1. one `FunctionCallResultWithProvenanceDeflated`, which links to the input IDs, the output ID, commit ID, build ID, and... 
2. one `FunctionCallWithProvenanceDeflated`, which knows the function, version, output class name, and references...
3. one `FunctionCallWithProvenance`, fully serialized, but with its input results fully deflated (see #1)

Note that the latter two are explicitly serialized as bytes.  The first is assembled from them, and other data stored
in the data fabric (described below).

A single digest ID can represent the entire history, and when histories intersect they data there is no duplication.


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
functions/com.cibo.provenance.Add/1.0/provenance-to-inputs/54b994cc3625bd54213e4dc9017b98c85cabedff/805b0523984a5b175938cfbdcd04015d6ee41ec4
functions/com.cibo.provenance.Add/1.0/provenance-values/690bd39b1116c99020ee054e2b7db238a76f0d19
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

##### `inputs-group-values`:
```
functions/com.cibo.provenance.Add/1.0/input-group-values/805b0523984a5b175938cfbdcd04015d6ee41ec4
```

The input-group-values path captures the assertion that the inputs `(1, 2)` are used as an input.  It is literally
the serialization of the Seq of inputs after converting them to digest strings.  This path has content, but is very
small:

##### `inputs-to-output`:
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


##### `provenance-values-typed` & `provenance-values-deflated`:
```
functions/com.cibo.provenance.Add/1.0/provenance-values-deflated/0f2bbc4919dd3e9272fb8f5b77058940d0b658eb
functions/com.cibo.provenance.Add/1.0/provenance-values-typed/690bd39b1116c99020ee054e2b7db238a76f0d19
```
The provenance-values-typed path captures all "full" `FunctionCallResultWithProvenance` after deflating all of the
inputs.

The provenance-values-deflated then captures the `FunctionCallResultWithProvenanceDeflated` made from it.  This 
keeps the ID of its full-formed predecessor.  

The deflated version is then used when recording this path as an input to other calls.  This lets us decomposes the 
full tree into separate objects so that long provenance does not result in larger and larger entries in the history 
chain.  The deflated version also only requires that the output type be instantiatable in the application, so it can 
bridge across libraries.

This is a small object, as the output value and input values are all stored as SHA1s in the core `data/` tree.

##### `output-to-provenance`:
```
functions/com.cibo.provenance.Add/1.0/output-to-provenance/cbf6eb1142cf44792e76a86e0d32fd89f94935a9/805b0523984a5b175938cfbdcd04015d6ee41ec4/54b994cc3625bd54213e4dc9017b98c85cabedff
```
This captures the FunctionCallResultWithProvenance to a given output/input pair.  This tree _will_ often have multiple
child paths under any parent path, when multiple inputs happen to produce the same outupt, or when multiple upstream
paths lead to the same inputs. 

##### `provenance-to-inputs`:
```
functions/com.cibo.provenance.Add/1.0/provenance-to-inputs/54b994cc3625bd54213e4dc9017b98c85cabedff/805b0523984a5b175938cfbdcd04015d6ee41ec4
```
This "completes the loop", allowing a given provenance object to look up its inputs.  The inputs can hypothetically be
known before the implementation executes, but they are typically only saved when other data is saved.


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

