Data Provenance Development
===========================

### Compiling and IntelliJ

This project uses sbt-boilerplate.  It will confuse IntelliJ briefly.

It generates all classes like Function{1..21}*:
- from: `src/main/boilarplate/**/*.scala.template`
- into: `target/scala-2.{11,12}/src_managed/**/*.scala`

If you are running IntelliJ and not having IntelliJ use sbt
to compile, it will find all of the template-based classes are missing
until after the first time you do `sbt +compile`.

If you modify any of the `.template` files, do another `sbt +compile` before resuming in IntelliJ.

### Publishing

This project is released as a library.
- To test locally, use `sbt +publishLocal`.
- To do a real release, use `sbt release` (requires CiBO credentials)

