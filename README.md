Data Provenance
===============

- Track function call input and output relationships.
- Separate function call data flow and execution.
- Prevent duplicate work.
- Manage storage.

Overview:
---------

A regular function might be defined like this:
```$scala
def foo(i: Int, d: Double): String =
    i.toString + "," + d.toString
```

More verbose:
```$scala
object foo extends Function2[String, Int, Double] {
    def apply(i: Int, d: Double): String =
        i.toString + "," + d.toString
}
```

Add data provenance tracking:
```$scala
object foo extends Function2WithProvenance[String, Int, Double] {
    val currentVersion = Version("0.1")
    def impl(i: Int, d: Double): String =
        i.toString + "," + d.toString
}
```

Use it:
```$scala
val f: foo.Call     = foo(5, 1.23)
val r: foo.Result   = f.run()
val o: String       = r.getOutputValue
```

The result knows where the output value came from:
```$scala
val f2: foo.Call    = r.getProvenanceValue
assert(f2 == f)
```

Save it:
```$scala
val db = ResultTrackerSimple("/my/datarepo")
db.saveResult(r)
```

Or see if someone else already did it:
```$scala
val r2: Option[foo.Result] = db.getResultOption(foo(5, 1.23))
```

Compose:
```$scala
object addInts extends Function2WithProvenance[Int, Int, Int] {
    val currentVersion = Version("0.1")
    def impl(a: Int, b: Int) = a + b
}

implicit db = ResultTrackerSimple("/my/data")

val s1 = addInts(10, addInts(6, 6))
val r1 = s1.resolve()
// ^^ calls 6+6 and 10+12
```
```
val s2 = addInts(10, addInts(5, 7))
val r2 = s2.resolve()
// ^^ calls 5+7, but skips calling 10+12

val s3 = addInts(5, addInts(5, addInts(3, 4))
val r3 = s3.resolve()
// ^^ calls 3+4, but skips 5+7 and 10+12 


```







